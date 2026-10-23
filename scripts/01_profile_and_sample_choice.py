# %% [markdown]
# # Профилирование данных и выбор доли выборки
#
# Исходные CSV распакованы в `data/interim/`. Скрипт смотрит на размеры и качество
# таблиц и подбирает долю клиентов для рабочей выборки.
# Выборка делается по клиентам (хеш `msno`), чтобы история клиента не разрывалась.

# %%
import duckdb
import pandas as pd

RAW = "/workspace/data/interim/"
PROFILE_LOGS = False  # полный проход по user_logs (30 ГБ) занимает около 10 минут
SHARES = [5, 10, 20]  # доли клиентов в процентах
MIN_SEGMENT = 1000  # сегмент значим, если в полном наборе в нём не меньше клиентов
LOGS_CLEAN = "total_secs BETWEEN 0 AND 86400"  # правило очистки логов: сутки максимум

con = duckdb.connect()
con.execute("SET memory_limit='4GB'; SET threads=2; SET enable_progress_bar=false")


def show(sql: str) -> None:
    """Выполняет запрос и печатает результат таблицей.

    Args:
        sql (str): текст SQL-запроса.
    """
    print(con.sql(sql).df().to_string(index=False), "\n")


def in_sample(share: int) -> str:
    """Возвращает условие SQL, выбирающее долю клиентов по хешу `msno`.

    Хеш md5 не зависит от версии DuckDB, поэтому выборка воспроизводима.

    Args:
        share (int): доля клиентов в процентах.

    Returns:
        str: условие для WHERE.
    """
    return f"md5_number_lower(msno) % 100 < {share}"


# %% [markdown]
# ## Загрузка
# Файлы `_v2` дополняют основные, поэтому транзакции объединяем.

# %%
con.execute(
    f"""
    CREATE TABLE members AS
    SELECT *
    FROM read_csv('{RAW}members_v3.csv')
    """
)
con.execute(
    f"""
    CREATE TABLE train AS
    SELECT *
    FROM read_csv('{RAW}train.csv')
    """
)
con.execute(
    f"""
    CREATE TABLE train_v2 AS
    SELECT *
    FROM read_csv('{RAW}data/churn_comp_refresh/train_v2.csv')
    """
)
con.execute(
    f"""
    CREATE TABLE tr AS
    SELECT *
    FROM read_csv('{RAW}transactions.csv')
    UNION ALL
    SELECT *
    FROM read_csv('{RAW}data/churn_comp_refresh/transactions_v2.csv')
    """
)

# %% [markdown]
# ## Метки и транзакции

# %%
show(
    """
    SELECT
        'train' AS table_name,
        count(*) AS rows_count,
        round(avg(is_churn), 4) AS churn_rate
    FROM train
    UNION ALL
    SELECT
        'train_v2',
        count(*),
        round(avg(is_churn), 4)
    FROM train_v2
    """
)
show(
    """
    SELECT
        count(*) AS rows_count,
        count(DISTINCT msno) AS clients,
        min(transaction_date) AS date_from,
        max(transaction_date) AS date_to
    FROM tr
    """
)

# %% [markdown]
# ## Качество members
# Пол указан в трети профилей, в `bd` пропуски закодированы нулём.

# %%
show(
    """
    SELECT
        count(*) AS clients,
        round(avg((gender IS NULL)::int), 3) AS gender_missing_share,
        round(avg((bd = 0)::int), 3) AS age_zero_share,
        round(avg((bd < 10 OR bd > 80)::int), 3) AS age_out_of_range_share
    FROM members
    """
)

# %% [markdown]
# ## Логи (опционально)
# Очистка: `total_secs` от 0 до 86 400. Вне диапазона около 0,05% строк.

# %%
if PROFILE_LOGS:
    show(
        f"""
        SELECT
            count(*) AS rows_count,
            approx_count_distinct(msno) AS clients,
            sum((total_secs < 0)::int) AS negative_secs,
            sum((total_secs > 86400)::int) AS over_day_secs
        FROM read_csv('{RAW}user_logs.csv')
        """
    )

# %% [markdown]
# ## Выбор доли выборки
# Сегмент: группа клиентов с одним значением категориального признака (способ регистрации,
# способ оплаты, длина тарифа). Для каждой доли считаем: сколько клиентов с метками,
# доля оттока и насколько просели значимые сегменты.


# %%
def segments(where: str = "TRUE") -> pd.DataFrame:
    """Считает размеры сегментов по клиентам.

    Args:
        where (str): условие SQL, ограничивающее клиентов (по умолчанию все).

    Returns:
        pd.DataFrame: столбцы `dim` (признак), `val` (значение), `n` (число клиентов).
    """
    return con.sql(
        f"""
        SELECT
            'registered_via' AS dim,
            registered_via::varchar AS val,
            count(*) AS n
        FROM members
        WHERE {where}
        GROUP BY 2
        UNION ALL
        SELECT
            'payment_method_id',
            payment_method_id::varchar,
            count(DISTINCT msno)
        FROM tr
        WHERE {where}
        GROUP BY 2
        UNION ALL
        SELECT
            'payment_plan_days',
            payment_plan_days::varchar,
            count(DISTINCT msno)
        FROM tr
        WHERE {where}
        GROUP BY 2
        """
    ).df()


full = segments().rename(columns={"n": "n_full"})
big = full[full.n_full >= MIN_SEGMENT]
print(f"Значимых сегментов (не меньше {MIN_SEGMENT} клиентов в полном наборе): {len(big)}\n")

rows = []
for share in SHARES:
    w = in_sample(share)
    s = big.merge(segments(w), on=["dim", "val"], how="left").fillna({"n": 0})
    lab = (
        con.sql(
            f"""
            WITH train_part AS (
                SELECT
                    count(*) AS train_n,
                    round(avg(is_churn), 4) AS train_churn
                FROM train
                WHERE {w}
            ),
            train_v2_part AS (
                SELECT
                    count(*) AS train_v2_n,
                    round(avg(is_churn), 4) AS train_v2_churn
                FROM train_v2
                WHERE {w}
            )
            SELECT
                train_n,
                train_churn,
                train_v2_n,
                train_v2_churn
            FROM train_part
            CROSS JOIN train_v2_part
            """
        )
        .df()
        .iloc[0]
    )
    rows.append(
        {
            "доля_%": share,
            "train": int(lab.train_n),
            "churn_train": lab.train_churn,
            "train_v2": int(lab.train_v2_n),
            "churn_train_v2": lab.train_v2_churn,
            "мин_сегмент": int(s.n.min()),
            "сегментов_меньше_100": int((s.n < 100).sum()),
        }
    )

print(pd.DataFrame(rows).to_string(index=False))
