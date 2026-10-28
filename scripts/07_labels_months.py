# %% [markdown]
# # Метки оттока для ряда месяцев
#
# Для скользящей проверки модели нужны метки не только февраля и марта 2017.
# Запрос разметки из `03_churn_labels.py` (логика соревнования KKBox) применяется к месяцам
# с января 2016 по февраль 2017: метка месяца M определяется транзакциями после начала M,
# поэтому последний месяц, который можно разметить по имеющимся транзакциям (данные до 31.03.2017), это февраль 2017.
# Март 2017 размечается только официальными метками (`train_v2`).
#
# Результат: `data/processed/labels_months.parquet` со столбцами `msno`, `expire_month`, `is_churn`, `source`:
# `own` (собственная разметка по транзакциям) и `official` (метки соревнования; февраль и март 2017).
# Для февраля 2017 есть обе разметки, это позволяет оценить расхождение собственной разметки.

# %%
import duckdb

PROCESSED = "/workspace/data/processed/"
OWN_MONTHS = [f"2016-{m:02d}-01" for m in range(1, 13)] + ["2017-01-01", "2017-02-01"]

con = duckdb.connect()
con.execute(
    "SET memory_limit='6GB'; SET threads=4; SET enable_progress_bar=false; "
    "SET temp_directory='/workspace/data/_duckdb_tmp'"
)
con.execute(
    f"""
    CREATE VIEW t AS
    SELECT
        msno,
        transaction_date AS td,
        membership_expire_date AS ex,
        is_cancel,
        plan_list_price::VARCHAR || payment_plan_days::VARCHAR || payment_method_id::VARCHAR AS sig,
        CASE
            WHEN is_cancel = 0 THEN membership_expire_date - DATE '1970-01-01'
            ELSE DATE '1970-01-01' - membership_expire_date
        END AS ex_key
    FROM '{PROCESSED}transactions.parquet'
    """
)


# %%
def churn_labels(month_start: str) -> str:
    """Строит запрос с оттоком клиентов, у которых подписка заканчивается в заданном месяце.

    Логика совпадает с `churn_labels` из `03_churn_labels.py` (вариант «вся история до начала месяца»).

    Args:
        month_start (str): первый день месяца в формате 'YYYY-MM-01'.

    Returns:
        str: SELECT-запрос со столбцами `msno` и `is_churn` (1: отток, 0: продление).
    """
    return f"""
    WITH last AS (
        SELECT
            msno,
            ex AS last_expire
        FROM t
        WHERE td < DATE '{month_start}'
        QUALIFY row_number() OVER (
            PARTITION BY msno
            ORDER BY td DESC, sig, is_cancel DESC, ex_key DESC
        ) = 1
    ),
    cand AS (
        SELECT *
        FROM last
        WHERE last_expire >= DATE '{month_start}'
            AND last_expire < DATE '{month_start}' + INTERVAL 1 MONTH
    ),
    fut AS (
        SELECT
            t.msno,
            t.td,
            t.is_cancel,
            t.ex,
            row_number() OVER (
                PARTITION BY t.msno
                ORDER BY t.td, t.sig DESC, t.is_cancel, t.ex_key
            ) AS rn,
            cand.last_expire
        FROM t
        JOIN cand USING (msno)
        WHERE t.td >= DATE '{month_start}'
    ),
    run AS (
        SELECT
            *,
            min(CASE WHEN is_cancel = 1 THEN ex END) OVER (
                PARTITION BY msno
                ORDER BY rn
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS cancel_min
        FROM fut
    ),
    renewal AS (
        SELECT
            msno,
            td,
            least(last_expire, coalesce(cancel_min, last_expire)) AS ref
        FROM run
        WHERE is_cancel = 0
        QUALIFY row_number() OVER (
            PARTITION BY msno
            ORDER BY rn
        ) = 1
    )
    SELECT
        cand.msno,
        CASE
            WHEN renewal.msno IS NULL THEN 1
            WHEN date_diff('day', renewal.ref, renewal.td) < 30 THEN 0
            ELSE 1
        END AS is_churn
    FROM cand
    LEFT JOIN renewal USING (msno)
    """


def build() -> None:
    """Сохраняет в parquet собственные метки месяцев из `OWN_MONTHS` и официальные метки."""
    con.execute("CREATE OR REPLACE TABLE labels_all (msno VARCHAR, expire_month DATE, is_churn INTEGER, source VARCHAR)")
    for month in OWN_MONTHS:
        con.execute(
            f"""
            INSERT INTO labels_all
            SELECT
                msno,
                DATE '{month}',
                is_churn,
                'own'
            FROM ({churn_labels(month)})
            """
        )
        print(month, "готово")
    con.execute(
        f"""
        INSERT INTO labels_all
        SELECT
            msno,
            expire_month,
            is_churn,
            'official'
        FROM '{PROCESSED}labels.parquet'
        """
    )
    con.execute(f"COPY labels_all TO '{PROCESSED}labels_months.parquet' (FORMAT parquet)")


build()

# %% [markdown]
# ## Проверка
# Клиентов и доля оттока по месяцам и источникам; для февраля 2017 расхождение собственной и официальной разметки.

# %%
LABELS = f"'{PROCESSED}labels_months.parquet'"
print(
    con.sql(
        f"""
        SELECT
            expire_month,
            source,
            count(*) AS clients,
            round(100 * avg(is_churn), 2) AS churn_pct
        FROM {LABELS}
        GROUP BY expire_month, source
        ORDER BY expire_month, source
        """
    )
    .df()
    .to_string(index=False)
)

# %%
print(
    con.sql(
        f"""
        WITH own AS (
            SELECT msno, is_churn
            FROM {LABELS}
            WHERE source = 'own' AND expire_month = DATE '2017-02-01'
        ),
        official AS (
            SELECT msno, is_churn
            FROM {LABELS}
            WHERE source = 'official' AND expire_month = DATE '2017-02-01'
        )
        SELECT
            count(*) AS common_clients,
            round(100 * avg((own.is_churn <> official.is_churn)::INT), 2) AS mismatch_pct
        FROM own
        JOIN official USING (msno)
        """
    )
    .df()
    .to_string(index=False)
)
