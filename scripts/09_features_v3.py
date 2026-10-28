# %% [markdown]
# # Признаки по скользящим окнам
#
# Накопленные по всей истории признаки (число транзакций, суммарные оплаты, стаж) растут вместе с календарём:
# данные начинаются в январе 2015, и чем позже месяц, тем длиннее наблюдаемая история клиентов. Признаки по окнам фиксированной длины
# перед датой прогноза D (первый день месяца окончания подписки) не имеют такого дрейфа по построению.
#
# Окна: 30, 60, 90, 180 и 365 дней до D. Для каждого окна: число транзакций, число отмен, число оплат, сумма оплат,
# доля автопродлений, число смен тарифа, число смен способа оплаты, число перерывов (окончание подписки с последующим разрывом
# от 30 дней). Использованы только транзакции с датой строго меньше D.
#
# Результат: `data/processed/features_v3.parquet`, одна строка на пару (клиент, месяц), только новые признаки;
# присоединяется к `features_v2.parquet` по (`msno`, `expire_month`).

# %%
import duckdb

PROCESSED = "/workspace/data/processed/"
MONTHS = [f"2016-{m:02d}-01" for m in range(1, 13)] + ["2017-01-01", "2017-02-01", "2017-03-01"]
WINDOWS = [30, 60, 90, 180, 365]
GAP_DAYS = 30  # разрыв между окончанием подписки и новой оплатой, считающийся перерывом

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
        payment_plan_days AS plan_days,
        actual_amount_paid AS paid,
        payment_method_id AS method,
        is_auto_renew AS auto_renew,
        plan_list_price::VARCHAR || payment_plan_days::VARCHAR || payment_method_id::VARCHAR AS sig
    FROM '{PROCESSED}transactions.parquet'
    """
)
con.execute(
    f"""
    CREATE VIEW labels AS
    SELECT *
    FROM '{PROCESSED}labels_months.parquet'
    """
)


# %%
def window_features(month: str, w: int) -> str:
    """Строит выражения SQL с признаками одного окна.

    Args:
        month (str): первый день месяца окончания подписки (момент прогноза D).
        w (int): длина окна в днях.

    Returns:
        str: список выражений для SELECT, перечисленных через запятую (агрегаты по таблице `seq` с историей клиента).
    """
    win = f"td >= DATE '{month}' - {w}"
    return f"""
        count(*) FILTER (WHERE {win}) AS n_tx_{w}d,
        coalesce(sum(is_cancel) FILTER (WHERE {win}), 0) AS n_cancel_{w}d,
        count(*) FILTER (WHERE {win} AND is_cancel = 0 AND paid > 0) AS n_paid_tx_{w}d,
        coalesce(sum(paid) FILTER (WHERE {win} AND is_cancel = 0), 0) AS total_paid_{w}d,
        avg(auto_renew) FILTER (WHERE {win} AND is_cancel = 0) AS share_auto_{w}d,
        coalesce(sum((plan_changed)::INT) FILTER (WHERE {win}), 0) AS n_plan_changes_{w}d,
        coalesce(sum((method_changed)::INT) FILTER (WHERE {win}), 0) AS n_method_changes_{w}d,
        coalesce(sum((lapse)::INT) FILTER (WHERE {win}), 0) AS n_lapses_{w}d
    """


def features_sql(month: str) -> str:
    """Строит запрос с признаками по окнам для клиентов, у которых подписка заканчивается в заданном месяце.

    Args:
        month (str): первый день месяца окончания подписки в формате 'YYYY-MM-01'.

    Returns:
        str: SELECT-запрос; использует временные таблицы `cl` (клиенты месяца) и `hist` (история клиентов до D).
    """
    exprs = ",\n".join(window_features(month, w) for w in WINDOWS)
    return f"""
    WITH seq AS (
        SELECT
            msno,
            td,
            is_cancel,
            paid,
            auto_renew,
            (is_cancel = 0 AND lag(plan_days) OVER w IS NOT NULL AND plan_days <> lag(plan_days) OVER w) AS plan_changed,
            (is_cancel = 0 AND lag(method) OVER w IS NOT NULL AND method <> lag(method) OVER w) AS method_changed,
            (is_cancel = 0 AND lag(ex) OVER w IS NOT NULL AND td - lag(ex) OVER w >= {GAP_DAYS}) AS lapse
        FROM hist
        WINDOW w AS (PARTITION BY msno ORDER BY td, ex, plan_days, method)
    ),
    agg AS (
        SELECT
            msno,
            {exprs}
        FROM seq
        GROUP BY msno
    )
    SELECT
        cl.msno,
        DATE '{month}' AS expire_month,
        agg.* EXCLUDE (msno)
    FROM cl
    LEFT JOIN agg USING (msno)
    """


def build() -> None:
    """Сохраняет в parquet признаки по окнам для всех месяцев из `MONTHS`, считая месяцы по очереди."""
    parts = []
    for month in MONTHS:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE cl AS
            SELECT DISTINCT msno
            FROM labels
            WHERE expire_month = DATE '{month}'
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE hist AS
            SELECT t.*
            FROM t
            JOIN cl USING (msno)
            WHERE t.td < DATE '{month}'
            """
        )
        target = f"{PROCESSED}features_v3_{month[:7]}.parquet"
        con.execute(f"COPY ({features_sql(month)}) TO '{target}' (FORMAT parquet)")
        parts.append(f"'{target}'")
        print(month, "готово")
    con.execute(
        f"COPY (SELECT * FROM read_parquet([{', '.join(parts)}])) TO '{PROCESSED}features_v3.parquet' (FORMAT parquet)"
    )


build()

# %% [markdown]
# ## Проверка
# Клиенты по месяцам, доля клиентов без транзакций в окнах, средние значения по месяцам для окон 90 и 365 дней
# (значения должны быть стабильны по месяцам, в отличие от накопленных признаков).

# %%
FEAT = f"'{PROCESSED}features_v3.parquet'"
print(
    con.sql(
        f"""
        SELECT
            expire_month,
            count(*) AS clients,
            round(100 * avg((n_tx_30d IS NULL)::INT), 2) AS no_tx_history_pct,
            round(avg(n_tx_90d), 2) AS avg_n_tx_90d,
            round(avg(n_tx_365d), 2) AS avg_n_tx_365d,
            round(avg(total_paid_365d), 0) AS avg_paid_365d,
            round(avg(n_lapses_365d), 3) AS avg_lapses_365d
        FROM {FEAT}
        GROUP BY expire_month
        ORDER BY expire_month
        """
    )
    .df()
    .to_string(index=False)
)
