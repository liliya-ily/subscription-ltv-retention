# %% [markdown]
# # Разметка оттока по транзакциям
#
# Логика соревнования KKBox (`WSDMChurnLabeller`), переписанная на SQL:
#
# 1. По истории до начала месяца находим дату окончания подписки после последней
#    транзакции. Порядок транзакций внутри дня: сначала подписка, потом отмена.
# 2. Кандидаты: клиенты, у которых эта дата попадает в рассматриваемый месяц.
# 3. Смотрим транзакции после начала месяца. Если подписка не продлена, клиент ушёл.
#    Иначе считаем разрыв в днях между датой окончания и первой новой подпиской
#    (отмена по пути может сдвинуть дату окончания раньше). Разрыв 30 дней и больше: отток.
#
# Проверка: разметка февраля 2017 сравнивается с метками `train.csv` (в `labels.parquet`).

# %%
import duckdb

PROCESSED = "/workspace/data/processed/"

con = duckdb.connect()
con.execute("SET memory_limit='4GB'; SET threads=2; SET enable_progress_bar=false")
con.execute(
    f"""CREATE VIEW t AS
    SELECT msno, transaction_date td, membership_expire_date ex, is_cancel,
           plan_list_price::VARCHAR || payment_plan_days::VARCHAR || payment_method_id::VARCHAR AS sig,
           CASE WHEN is_cancel = 0 THEN ex - DATE '1970-01-01'
                ELSE DATE '1970-01-01' - ex END AS ex_key
    FROM '{PROCESSED}transactions.parquet'"""
)


# %%
def churn_labels(month_start: str, history_from: str | None = None) -> str:
    """Строит запрос с оттоком клиентов, у которых подписка заканчивается в заданном месяце.

    Args:
        month_start (str): первый день месяца в формате 'YYYY-MM-01'.
        history_from (str | None): дата, с которой учитывается история
            (по умолчанию вся история до начала месяца).

    Returns:
        str: SELECT-запрос со столбцами `msno` и `is_churn` (1: отток, 0: продление).
    """
    hist = f"td < DATE '{month_start}'"
    if history_from:
        hist += f" AND td >= DATE '{history_from}'"
    return f"""
    WITH last AS (
        SELECT msno, ex AS last_expire FROM t WHERE {hist}
        QUALIFY row_number() OVER (
            PARTITION BY msno ORDER BY td DESC, sig, is_cancel DESC, ex_key DESC) = 1),
    cand AS (
        SELECT * FROM last
        WHERE last_expire >= DATE '{month_start}'
          AND last_expire < DATE '{month_start}' + INTERVAL 1 MONTH),
    fut AS (
        SELECT t.msno, t.td, t.is_cancel, t.ex,
               row_number() OVER (
                   PARTITION BY t.msno ORDER BY t.td, t.sig DESC, t.is_cancel, t.ex_key) AS rn,
               cand.last_expire
        FROM t JOIN cand USING (msno) WHERE t.td >= DATE '{month_start}'),
    run AS (
        SELECT *, min(CASE WHEN is_cancel = 1 THEN ex END) OVER (
                   PARTITION BY msno ORDER BY rn
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cancel_min
        FROM fut),
    renewal AS (
        SELECT msno, td, least(last_expire, coalesce(cancel_min, last_expire)) AS ref
        FROM run WHERE is_cancel = 0
        QUALIFY row_number() OVER (PARTITION BY msno ORDER BY rn) = 1)
    SELECT cand.msno,
           CASE WHEN renewal.msno IS NULL THEN 1
                WHEN date_diff('day', renewal.ref, renewal.td) < 30 THEN 0
                ELSE 1 END AS is_churn
    FROM cand LEFT JOIN renewal USING (msno)"""


# %% [markdown]
# ## Сверка с `train.csv` (февраль 2017)
# Вариант «вся история» против варианта «только январь» (как в упрощённом скрипте KKBox).

# %%
con.execute(
    f"""CREATE TABLE train AS SELECT msno, is_churn FROM '{PROCESSED}labels.parquet'
    WHERE expire_month = DATE '2017-02-01'"""
)
for name, history_from in [("вся история", None), ("только январь", "2017-01-01")]:
    con.execute(f"CREATE OR REPLACE TABLE mine AS {churn_labels('2017-02-01', history_from)}")
    print(name)
    print(
        con.sql(
            """SELECT
            (SELECT count(*) FROM mine) AS ours_n,
            (SELECT count(*) FROM train) AS kkbox_n,
            count(*) AS common_n,
            sum((m.is_churn <> tr.is_churn)::int) AS label_mismatches,
            (SELECT count(*) FROM mine WHERE msno NOT IN (SELECT msno FROM train)) AS only_ours,
            (SELECT count(*) FROM train WHERE msno NOT IN (SELECT msno FROM mine)) AS only_kkbox,
            round((SELECT avg(is_churn) FROM mine), 4) AS ours_churn_rate
            FROM mine m JOIN train tr USING (msno)"""
        )
        .df()
        .to_string(index=False),
        "\n",
    )
