# %% [markdown]
# # Расширенные признаки для скользящей проверки
#
# Признаки считаются для всех месяцев из `labels_months.parquet` (с января 2016 по март 2017) на момент D,
# первый день месяца окончания подписки. Как в `05_churn_features.py`, используются только транзакции строго
# до D, профиль берётся на D. К прежним 32 признакам добавлены признаки по мотивам разборов соревнования KKBox:
#
# - предпоследняя транзакция и смены тарифа, способа оплаты и автопродления относительно последней;
# - интервал между последними двумя транзакциями;
# - интервал «дата окончания минус дата транзакции» и его разность с длиной тарифа (накопленные подписки);
# - цена и прайс за день, доля скидки;
# - статистика по последним пяти транзакциям;
# - число прошедших окончаний подписки и доля окончаний, за которыми был перерыв.
#
# Метки в таблицу не входят: они лежат в `labels_months.parquet` и присоединяются по (`msno`, `expire_month`).
# Результат: `data/processed/features_v2.parquet`, одна строка на пару (клиент, месяц).

# %%
import duckdb

PROCESSED = "/workspace/data/processed/"
MONTHS = [f"2016-{m:02d}-01" for m in range(1, 13)] + ["2017-01-01", "2017-02-01", "2017-03-01"]
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
    FROM '{PROCESSED}labels_months.parquet'
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
        str: SELECT-запрос; использует временные таблицы `cl` (клиенты месяца) и `hist`
            (история клиентов до D с рангом транзакции `rk`, 1: последняя).
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
        WHERE rk = 1
    ),
    prev AS (
        SELECT
            msno,
            td AS prev_td,
            plan_days AS prev_plan_days,
            paid AS prev_paid,
            method AS prev_method,
            auto_renew AS prev_auto_renew,
            is_cancel AS prev_is_cancel
        FROM hist
        WHERE rk = 2
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
            count(*) FILTER (WHERE td >= DATE '{month}' - 90) AS n_tx_90d,
            sum(is_cancel) FILTER (WHERE rk <= 5) AS n_cancel_5,
            avg(paid) FILTER (WHERE rk <= 5 AND is_cancel = 0) AS avg_paid_5,
            avg(plan_days) FILTER (WHERE rk <= 5 AND is_cancel = 0) AS avg_plan_days_5,
            avg(auto_renew) FILTER (WHERE rk <= 5 AND is_cancel = 0) AS share_auto_5,
            count(*) FILTER (WHERE is_cancel = 0 AND ex < DATE '{month}') AS n_expired
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
            lag(method) OVER w AS prev_method_seq,
            lag(ex) OVER w AS prev_ex
        FROM hist
        WHERE is_cancel = 0
        WINDOW w AS (PARTITION BY msno ORDER BY td, ex, plan_days, method)
    ),
    changes AS (
        SELECT
            msno,
            sum((prev_plan IS NOT NULL AND plan_days <> prev_plan)::INT) AS n_plan_changes,
            sum((prev_method_seq IS NOT NULL AND method <> prev_method_seq)::INT) AS n_method_changes,
            sum((prev_ex IS NOT NULL AND td - prev_ex >= {GAP_DAYS})::INT) AS n_lapses
        FROM seq
        GROUP BY msno
    )
    SELECT
        cl.msno,
        DATE '{month}' AS expire_month,
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
        END AS reg_tenure_days,
        prev.prev_plan_days,
        prev.prev_paid,
        prev.prev_method,
        prev.prev_auto_renew,
        prev.prev_is_cancel,
        (last.last_plan_days <> prev.prev_plan_days)::INT AS plan_changed,
        (last.last_method <> prev.prev_method)::INT AS method_changed,
        (last.last_auto_renew <> prev.prev_auto_renew)::INT AS auto_renew_changed,
        date_diff('day', prev.prev_td, last.last_td) AS days_between_last_two_tx,
        date_diff('day', last.last_td, last.last_ex) AS ex_minus_td,
        date_diff('day', last.last_td, last.last_ex) - last.last_plan_days AS ex_minus_td_minus_plan,
        last.last_paid::DOUBLE / nullif(last.last_plan_days, 0) AS paid_per_day,
        last.last_list_price::DOUBLE / nullif(last.last_plan_days, 0) AS list_per_day,
        (last.last_list_price - last.last_paid)::DOUBLE / nullif(last.last_list_price, 0) AS discount_ratio,
        agg.n_cancel_5,
        agg.avg_paid_5,
        agg.avg_plan_days_5,
        agg.share_auto_5,
        agg.n_expired,
        changes.n_lapses::DOUBLE / nullif(agg.n_expired, 0) AS lapse_share
    FROM cl
    LEFT JOIN last USING (msno)
    LEFT JOIN prev USING (msno)
    LEFT JOIN agg USING (msno)
    LEFT JOIN changes USING (msno)
    LEFT JOIN mem USING (msno)
    """


def build() -> None:
    """Сохраняет в parquet признаки всех месяцев из `MONTHS`, считая месяцы по очереди."""
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
            SELECT
                t.*,
                row_number() OVER (
                    PARTITION BY t.msno
                    ORDER BY t.td DESC, t.sig, t.is_cancel DESC, t.ex_key DESC
                ) AS rk
            FROM t
            JOIN cl USING (msno)
            WHERE t.td < DATE '{month}'
            """
        )
        target = f"{PROCESSED}features_v2_{month[:7]}.parquet"
        con.execute(f"COPY ({features_sql(month)}) TO '{target}' (FORMAT parquet)")
        parts.append(f"'{target}'")
        print(month, "готово")
    con.execute(
        f"COPY (SELECT * FROM read_parquet([{', '.join(parts)}])) TO '{PROCESSED}features_v2.parquet' (FORMAT parquet)"
    )


build()

# %% [markdown]
# ## Проверка
# Клиентов по месяцам, клиенты без истории до D, отсутствие использованных транзакций не раньше D
# (минимум дней с последней транзакции не меньше 1), доля пропусков новых признаков.

# %%
FEAT = f"'{PROCESSED}features_v2.parquet'"
print(
    con.sql(
        f"""
        SELECT
            expire_month,
            count(*) AS clients,
            count(*) FILTER (WHERE n_tx = 0) AS without_history,
            min(days_since_last_tx) AS min_days_since_last_tx,
            round(100 * avg((prev_plan_days IS NULL)::INT), 2) AS no_prev_tx_pct,
            round(100 * avg((ex_minus_td_minus_plan IS NULL)::INT), 2) AS no_gap_pct
        FROM {FEAT}
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
            round(avg(ex_minus_td_minus_plan), 1) AS avg_gap_minus_plan,
            round(100 * avg((ex_minus_td_minus_plan > 30)::INT), 2) AS share_accumulated_pct,
            round(avg(paid_per_day), 2) AS avg_paid_per_day,
            round(avg(lapse_share), 3) AS avg_lapse_share
        FROM {FEAT}
        """
    )
    .df()
    .to_string(index=False)
)
