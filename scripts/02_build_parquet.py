# %% [markdown]
# # Подготовка рабочих данных
#
# Транзакции, участники и метки сохраняются целиком: их объём позволяет считать
# когорты, LTV и сегменты без выборки. Логи прослушивания (около 410 млн строк)
# сохраняются только для доли клиентов `SAMPLE_SHARE`, выбранной в
# `01_profile_and_sample_choice.py`.
#
# Результат: `data/processed/*.parquet`, даты приведены к типу DATE.

# %%
import os

import duckdb

RAW = "/workspace/data/interim/"
REFRESH = RAW + "data/churn_comp_refresh/"
OUT = "/workspace/data/processed/"
SAMPLE_SHARE = 10  # процент клиентов для логов
LOGS_CLEAN = "total_secs BETWEEN 0 AND 86400"

os.makedirs(OUT, exist_ok=True)
con = duckdb.connect()
con.execute("SET memory_limit='4GB'; SET threads=2; SET enable_progress_bar=false")


def to_date(col: str) -> str:
    """Возвращает выражение SQL, приводящее столбец формата YYYYMMDD к типу DATE.

    Args:
        col (str): имя столбца.

    Returns:
        str: выражение с тем же именем столбца в результате.
    """
    return f"try_strptime(CAST({col} AS VARCHAR), '%Y%m%d')::DATE AS {col}"


def save(name: str, sql: str) -> None:
    """Сохраняет результат запроса в parquet и печатает число строк.

    Args:
        name (str): имя файла без расширения в `OUT`.
        sql (str): текст SELECT-запроса.
    """
    con.execute(f"COPY ({sql}) TO '{OUT}{name}.parquet' (FORMAT parquet)")
    n = con.sql(f"SELECT count(*) FROM '{OUT}{name}.parquet'").fetchone()[0]
    print(f"{name}: {n:,} строк")


# %% [markdown]
# ## Участники и метки

# %%
save(
    "members",
    f"""
    SELECT * EXCLUDE (registration_init_time), {to_date('registration_init_time')}
    FROM read_csv('{RAW}members_v3.csv')""",
)
save(
    "labels",
    f"""
    SELECT msno, is_churn, DATE '2017-02-01' AS expire_month FROM read_csv('{RAW}train.csv')
    UNION ALL
    SELECT msno, is_churn, DATE '2017-03-01' FROM read_csv('{REFRESH}train_v2.csv')""",
)

# %% [markdown]
# ## Транзакции
# Основной файл и `_v2` дополняют друг друга, поэтому объединяются.

# %%
save(
    "transactions",
    f"""
    SELECT * EXCLUDE (transaction_date, membership_expire_date),
           {to_date('transaction_date')}, {to_date('membership_expire_date')}
    FROM (
        SELECT * FROM read_csv('{RAW}transactions.csv')
        UNION ALL
        SELECT * FROM read_csv('{REFRESH}transactions_v2.csv'))""",
)

# %% [markdown]
# ## Логи по выборке клиентов
# Выборка по хешу `msno`; очистка: `total_secs` от 0 до 86 400 секунд.

# %%
save(
    "user_logs_sample",
    f"""
    SELECT * EXCLUDE (date), {to_date('date')}
    FROM (
        SELECT * FROM read_csv('{RAW}user_logs.csv')
        UNION ALL
        SELECT * FROM read_csv('{REFRESH}user_logs_v2.csv'))
    WHERE md5_number_lower(msno) % 100 < {SAMPLE_SHARE} AND {LOGS_CLEAN}""",
)

# %% [markdown]
# ## Проверка дат
# Нераспознанные даты превращаются в NULL; их число должно быть нулевым.

# %%
print(
    con.sql(
        f"""SELECT
    (SELECT count(*) FROM '{OUT}transactions.parquet'
        WHERE transaction_date IS NULL OR membership_expire_date IS NULL) AS tr_null_dates,
    (SELECT count(*) FROM '{OUT}members.parquet' WHERE registration_init_time IS NULL) AS members_null_dates
    """
    )
    .df()
    .to_string(index=False)
)
