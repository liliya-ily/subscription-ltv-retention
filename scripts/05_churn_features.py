# %% [markdown]
# # Признаки для модели оттока
#
# Объект: клиент с официальной меткой (`labels.parquet`), у которого подписка заканчивается
# в месяце M (февраль 2017: `train`, март 2017: `train_v2`).
#
# Момент прогноза D: первый день месяца M. Признаки считаются только по транзакциям
# с датой строго меньше D, поэтому информация из месяца окончания подписки, по которому
# строится метка, в признаки не попадает. Профиль берётся на D: давность регистрации
# для регистраций позже D считается пропуском.
#
# Состояние подписки определяется по последней транзакции до D с тем же порядком
# сортировки, что в `03_churn_labels.py`.
#
# Результат: `data/processed/churn_features.parquet`, одна строка на пару (клиент, месяц).

# %%
import duckdb

PROCESSED = "/workspace/data/processed/"
MONTHS = ["2017-02-01", "2017-03-01"]
GAP_DAYS = 30  # разрыв между окончанием подписки и новой оплатой, считающийся перерывом

con = duckdb.connect()
con.execute(
    "SET memory_limit='4GB'; SET threads=2; SET enable_progress_bar=false; "
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
        plan_list_price AS list_price,
        actual_amount_paid AS paid,
        payment_method_id AS method,
        is_auto_renew AS auto_renew,
        plan_list_price::VARCHAR || payment_plan_days::VARCHAR || payment_method_id::VARCHAR AS sig,
        CASE
            WHEN is_cancel = 0 THEN membership_expire_date - DATE '1970-01-01'
            ELSE DATE '1970-01-01' - membership_expire_date
        END AS ex_key
    FROM '{PROCESSED}transactions.parquet'
    """
)
con.execute(
    f"""
    CREATE VIEW labels AS
    SELECT *
    FROM '{PROCESSED}labels.parquet'
    """
)
con.execute(
    f"""
    CREATE VIEW mem AS
    SELECT *
    FROM '{PROCESSED}members.parquet'
    """
)


# %%
def features_sql(month: str) -> str:
    """Строит запрос с признаками клиентов, у которых подписка заканчивается в заданном месяце.

    Args:
        month (str): первый день месяца окончания подписки (момент прогноза D) в формате 'YYYY-MM-01'.

    Returns:
        str: SELECT-запрос; использует временную таблицу `hist` с историей клиентов до D.
    """
    return f"""
    WITH last AS (
        SELECT
            msno,
            td AS last_td,
            ex AS last_ex,
            plan_days AS last_plan_days,
            list_price AS last_list_price,
            paid AS last_paid,
            method AS last_method,
            auto_renew AS last_auto_renew,
            is_cancel AS last_is_cancel
        FROM hist
        QUALIFY row_number() OVER (
            PARTITION BY msno
            ORDER BY td DESC, sig, is_cancel DESC, ex_key DESC
        ) = 1
    ),
    agg AS (
        SELECT
            msno,
            count(*) AS n_tx,
            sum(is_cancel) AS n_cancel,
            min(td) AS first_td,
            count(*) FILTER (WHERE is_cancel = 0 AND paid > 0) AS n_paid_tx,
            sum(paid) FILTER (WHERE is_cancel = 0) AS total_paid,
            avg(paid) FILTER (WHERE is_cancel = 0 AND paid > 0) AS avg_paid,
            avg((paid = 0)::INT) FILTER (WHERE is_cancel = 0) AS share_zero_paid,
            avg((paid > 0 AND paid < list_price)::INT) FILTER (WHERE is_cancel = 0) AS share_discount,
            avg(auto_renew) FILTER (WHERE is_cancel = 0) AS share_auto_renew,
            count(DISTINCT plan_days) FILTER (WHERE is_cancel = 0) AS n_plans,
            count(DISTINCT method) FILTER (WHERE is_cancel = 0) AS n_methods,
            count(*) FILTER (WHERE td >= DATE '{month}' - 30) AS n_tx_30d,
            count(*) FILTER (WHERE td >= DATE '{month}' - 90) AS n_tx_90d
        FROM hist
        GROUP BY msno
    ),
    seq AS (
        SELECT
            msno,
            td,
            plan_days,
            method,
            lag(plan_days) OVER w AS prev_plan,
            lag(method) OVER w AS prev_method,
            lag(ex) OVER w AS prev_ex
        FROM hist
        WHERE is_cancel = 0
        WINDOW w AS (PARTITION BY msno ORDER BY td, ex, plan_days, method)
    ),
    changes AS (
        SELECT
            msno,
            sum((prev_plan IS NOT NULL AND plan_days <> prev_plan)::INT) AS n_plan_changes,
            sum((prev_method IS NOT NULL AND method <> prev_method)::INT) AS n_method_changes,
            sum((prev_ex IS NOT NULL AND td - prev_ex >= {GAP_DAYS})::INT) AS n_lapses
        FROM seq
        GROUP BY msno
    )
    SELECT
        labels.msno,
        DATE '{month}' AS expire_month,
        labels.is_churn,
        last.last_plan_days,
        last.last_list_price,
        last.last_paid,
        last.last_method,
        last.last_auto_renew,
        last.last_is_cancel,
        (last.last_paid > 0 AND last.last_paid < last.last_list_price)::INT AS last_discount,
        date_diff('day', DATE '{month}', last.last_ex) AS days_to_expire,
        date_diff('day', last.last_td, DATE '{month}') AS days_since_last_tx,
        date_diff('day', agg.first_td, DATE '{month}') AS tenure_days,
        coalesce(agg.n_tx, 0) AS n_tx,
        agg.n_cancel,
        agg.n_cancel::DOUBLE / agg.n_tx AS share_cancel,
        agg.n_paid_tx,
        agg.total_paid,
        agg.avg_paid,
        agg.share_zero_paid,
        agg.share_discount,
        agg.share_auto_renew,
        agg.n_plans,
        agg.n_methods,
        agg.n_tx_30d,
        agg.n_tx_90d,
        changes.n_plan_changes,
        changes.n_method_changes,
        changes.n_lapses,
        (mem.msno IS NOT NULL)::INT AS has_profile,
        mem.city,
        mem.gender,
        CASE WHEN mem.bd BETWEEN 10 AND 90 THEN mem.bd END AS age,
        mem.registered_via,
        CASE
            WHEN mem.registration_init_time <= DATE '{month}'
            THEN date_diff('day', mem.registration_init_time, DATE '{month}')
        END AS reg_tenure_days
    FROM labels
    LEFT JOIN last USING (msno)
    LEFT JOIN agg USING (msno)
    LEFT JOIN changes USING (msno)
    LEFT JOIN mem USING (msno)
    WHERE labels.expire_month = DATE '{month}'
    """


def build() -> None:
    """Сохраняет в parquet признаки всех месяцев из `MONTHS`, считая месяцы по очереди."""
    for month in MONTHS:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE hist AS
            SELECT t.*
            FROM t
            JOIN labels USING (msno)
            WHERE labels.expire_month = DATE '{month}'
                AND t.td < DATE '{month}'
            """
        )
        target = f"'{PROCESSED}churn_features_{month[:7]}.parquet'"
        con.execute(f"COPY ({features_sql(month)}) TO {target} (FORMAT parquet)")
        print(month, "готово")
    parts = ", ".join(f"'{PROCESSED}churn_features_{m[:7]}.parquet'" for m in MONTHS)
    con.execute(
        f"COPY (SELECT * FROM read_parquet([{parts}])) TO '{PROCESSED}churn_features.parquet' (FORMAT parquet)"
    )


build()

# %% [markdown]
# ## Проверка
# Размер и доля оттока по месяцам (официальные доли: февраль 6,4%, март 9,0%), клиенты без истории до D,
# отсутствие транзакций с датой не раньше D среди использованных, доля пропусков по признакам.

# %%
FEAT = f"'{PROCESSED}churn_features.parquet'"
print(
    con.sql(
        f"""
        SELECT
            expire_month,
            count(*) AS clients,
            round(100 * avg(is_churn), 2) AS churn_pct,
            count(*) FILTER (WHERE n_tx = 0) AS without_history,
            count(*) FILTER (WHERE last_td_check >= expire_month) AS leak_rows
        FROM (
            SELECT
                *,
                expire_month - days_since_last_tx::INT AS last_td_check
            FROM {FEAT}
        )
        GROUP BY expire_month
        ORDER BY expire_month
        """
    )
    .df()
    .to_string(index=False)
)

# %%
print(
    con.sql(
        f"""
        SELECT
            round(100 * avg((days_to_expire IS NULL)::INT), 2) AS no_last_tx_pct,
            round(100 * avg((city IS NULL)::INT), 2) AS no_profile_pct,
            round(100 * avg((age IS NULL)::INT), 2) AS no_age_pct,
            round(100 * avg((reg_tenure_days IS NULL)::INT), 2) AS no_reg_tenure_pct,
            min(days_to_expire) AS min_days_to_expire,
            max(days_to_expire) AS max_days_to_expire,
            min(days_since_last_tx) AS min_days_since_last_tx
        FROM {FEAT}
        """
    )
    .df()
    .to_string(index=False)
)
