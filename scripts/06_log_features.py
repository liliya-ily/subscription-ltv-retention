# %% [markdown]
# # Признаки из логов прослушивания
#
# Логи сохранены для выборки клиентов (`SAMPLE_SHARE` процентов по хешу `msno`, см. `02_build_parquet.py`),
# поэтому признаки считаются только для клиентов выборки с официальной меткой.
#
# Момент прогноза D тот же, что в `05_churn_features.py`: первый день месяца окончания подписки.
# Используются только логи с датой строго меньше D (логи самого месяца окончания относятся к будущему):
# три окна по 30 дней перед D (`l30`: дни D-30..D-1, `l60`: D-60..D-31, `l90`: D-90..D-61).
# Клиент выборки без строк логов в окне считается не слушавшим: счётчики равны нулю, доли и отношения пропущены.
#
# Результат: `data/processed/log_features.parquet`, одна строка на пару (клиент, месяц).

# %%
import duckdb

PROCESSED = "/workspace/data/processed/"
MONTHS = ["2017-02-01", "2017-03-01"]
SAMPLE_SHARE = 10  # процент клиентов, для которых сохранены логи
LOGS_CLEAN = "total_secs BETWEEN 0 AND 86400"

con = duckdb.connect()
con.execute(
    "SET memory_limit='4GB'; SET threads=2; SET enable_progress_bar=false; "
    "SET temp_directory='/workspace/data/_duckdb_tmp'"
)
con.execute(
    f"""
    CREATE VIEW logs AS
    SELECT *
    FROM '{PROCESSED}user_logs_sample.parquet'
    WHERE {LOGS_CLEAN}
    """
)
con.execute(
    f"""
    CREATE VIEW labels AS
    SELECT *
    FROM '{PROCESSED}labels.parquet'
    """
)


# %%
def window_aggs(w: int, name: str) -> str:
    """Строит выражения SQL с признаками одного окна.

    Args:
        w (int): номер окна в таблице `lg` (1: последние 30 дней, 2: предыдущие 30, 3: ещё 30).
        name (str): префикс признаков (`l30`, `l60`, `l90`).

    Returns:
        str: список выражений для SELECT, перечисленных через запятую.
    """
    plays = "(num_25 + num_50 + num_75 + num_985 + num_100)"
    return f"""
        count(DISTINCT date) FILTER (WHERE w = {w}) AS {name}_days,
        coalesce(sum({plays}) FILTER (WHERE w = {w}), 0) AS {name}_plays,
        coalesce(sum(total_secs) FILTER (WHERE w = {w}), 0) AS {name}_secs,
        coalesce(sum(num_unq) FILTER (WHERE w = {w}), 0) AS {name}_unq,
        sum(num_100) FILTER (WHERE w = {w})::DOUBLE
            / nullif(sum({plays}) FILTER (WHERE w = {w}), 0) AS {name}_full_share,
        sum(num_25) FILTER (WHERE w = {w})::DOUBLE
            / nullif(sum({plays}) FILTER (WHERE w = {w}), 0) AS {name}_skip_share
    """


def features_sql(month: str) -> str:
    """Строит запрос с признаками из логов для клиентов выборки, у которых подписка заканчивается в месяце.

    Args:
        month (str): первый день месяца окончания подписки (момент прогноза D) в формате 'YYYY-MM-01'.

    Returns:
        str: SELECT-запрос.
    """
    return f"""
    WITH cl AS (
        SELECT msno
        FROM labels
        WHERE expire_month = DATE '{month}'
            AND md5_number_lower(msno) % 100 < {SAMPLE_SHARE}
    ),
    lg AS (
        SELECT
            logs.*,
            CASE
                WHEN date >= DATE '{month}' - 30 THEN 1
                WHEN date >= DATE '{month}' - 60 THEN 2
                ELSE 3
            END AS w
        FROM logs
        JOIN cl USING (msno)
        WHERE date < DATE '{month}'
            AND date >= DATE '{month}' - 90
    ),
    agg AS (
        SELECT
            msno,
            {window_aggs(1, 'l30')},
            {window_aggs(2, 'l60')},
            {window_aggs(3, 'l90')},
            date_diff('day', max(date), DATE '{month}') AS days_since_last_listen
        FROM lg
        GROUP BY msno
    )
    SELECT
        cl.msno,
        DATE '{month}' AS expire_month,
        coalesce(agg.l30_days, 0) AS l30_days,
        coalesce(agg.l30_plays, 0) AS l30_plays,
        coalesce(agg.l30_secs, 0) AS l30_secs,
        coalesce(agg.l30_unq, 0) AS l30_unq,
        agg.l30_full_share,
        agg.l30_skip_share,
        coalesce(agg.l60_days, 0) AS l60_days,
        coalesce(agg.l60_plays, 0) AS l60_plays,
        coalesce(agg.l60_secs, 0) AS l60_secs,
        coalesce(agg.l60_unq, 0) AS l60_unq,
        agg.l60_full_share,
        agg.l60_skip_share,
        coalesce(agg.l90_days, 0) AS l90_days,
        coalesce(agg.l90_plays, 0) AS l90_plays,
        coalesce(agg.l90_secs, 0) AS l90_secs,
        coalesce(agg.l90_unq, 0) AS l90_unq,
        agg.l90_full_share,
        agg.l90_skip_share,
        coalesce(agg.l30_secs, 0) / nullif(agg.l30_days, 0) AS l30_secs_per_day,
        coalesce(agg.l30_secs, 0)::DOUBLE / nullif(agg.l60_secs, 0) AS secs_ratio_30_60,
        coalesce(agg.l30_days, 0)::DOUBLE / nullif(agg.l60_days, 0) AS days_ratio_30_60,
        agg.days_since_last_listen
    FROM cl
    LEFT JOIN agg USING (msno)
    """


def build() -> None:
    """Сохраняет в parquet признаки из логов для всех месяцев из `MONTHS`."""
    parts = []
    for month in MONTHS:
        target = f"{PROCESSED}log_features_{month[:7]}.parquet"
        con.execute(f"COPY ({features_sql(month)}) TO '{target}' (FORMAT parquet)")
        parts.append(f"'{target}'")
        print(month, "готово")
    con.execute(
        f"COPY (SELECT * FROM read_parquet([{', '.join(parts)}])) TO '{PROCESSED}log_features.parquet' (FORMAT parquet)"
    )


build()

# %% [markdown]
# ## Проверка
# Клиенты выборки по месяцам, доля клиентов без логов в 90 днях до D, отсутствие логов не раньше D среди использованных
# (минимальное число дней с последнего прослушивания должно быть не меньше 1), доля пропусков признаков.

# %%
FEAT = f"'{PROCESSED}log_features.parquet'"
print(
    con.sql(
        f"""
        SELECT
            expire_month,
            count(*) AS clients,
            round(100 * avg((l30_days + l60_days + l90_days = 0)::INT), 2) AS no_logs_pct,
            round(100 * avg((l30_days = 0)::INT), 2) AS no_logs_30d_pct,
            min(days_since_last_listen) AS min_days_since_last_listen,
            max(days_since_last_listen) AS max_days_since_last_listen,
            round(avg(l30_days), 2) AS avg_l30_days,
            round(avg(l30_secs) / 3600, 2) AS avg_l30_hours
        FROM {FEAT}
        GROUP BY expire_month
        ORDER BY expire_month
        """
    )
    .df()
    .to_string(index=False)
)
