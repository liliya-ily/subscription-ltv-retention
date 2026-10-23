# %% [markdown]
# # Активные месяцы и привязки когорт
#
# Клиент активен в месяце, если месяц пересекается с интервалом действия подписки:
# от даты транзакции до `membership_expire_date`. Учитываются транзакции без отмен;
# дата окончания обрезается концом наблюдений (в данных встречаются даты до 2036 года).
#
# Результат (`data/processed/`):
# - `active_months.parquet`: клиент и месяц, в котором подписка действовала;
# - `user_anchors.parquet`: месяц регистрации, месяц первой подписки (включая пробную)
#   и месяц первой платной подписки (сумма оплаты больше нуля).

# %%
import duckdb

PROCESSED = "/workspace/data/processed/"
DATA_END = "2017-03-31"  # последний день наблюдений

con = duckdb.connect()
con.execute(
    "SET memory_limit='4GB'; SET threads=2; SET enable_progress_bar=false; "
    "SET temp_directory='/workspace/data/_duckdb_tmp'"
)
con.execute(
    f"""
    CREATE VIEW tr AS
    SELECT *
    FROM '{PROCESSED}transactions.parquet'
    """
)
con.execute(
    f"""
    CREATE VIEW members AS
    SELECT *
    FROM '{PROCESSED}members.parquet'
    """
)


# %%
def build_active_months() -> None:
    """Сохраняет в parquet пары «клиент, месяц», когда у клиента действовала подписка."""
    con.execute(
        f"""
        COPY (
            SELECT DISTINCT
                msno,
                unnest(generate_series(
                    date_trunc('month', transaction_date),
                    date_trunc('month', least(membership_expire_date, DATE '{DATA_END}')),
                    INTERVAL 1 MONTH
                ))::DATE AS month
            FROM tr
            WHERE is_cancel = 0
                AND membership_expire_date >= transaction_date
                AND transaction_date <= DATE '{DATA_END}'
        ) TO '{PROCESSED}active_months.parquet' (FORMAT parquet)
        """
    )


def build_user_anchors() -> None:
    """Сохраняет в parquet месяцы регистрации, первой подписки и первой платной подписки."""
    con.execute(
        f"""
        COPY (
            WITH subs AS (
                SELECT
                    msno,
                    date_trunc('month', min(transaction_date))::DATE AS first_sub_month,
                    date_trunc(
                        'month',
                        min(transaction_date) FILTER (WHERE actual_amount_paid > 0)
                    )::DATE AS first_paid_month
                FROM tr
                WHERE is_cancel = 0
                GROUP BY msno
            )
            SELECT
                msno,
                date_trunc('month', members.registration_init_time)::DATE AS reg_month,
                subs.first_sub_month,
                subs.first_paid_month
            FROM members
            FULL JOIN subs USING (msno)
        ) TO '{PROCESSED}user_anchors.parquet' (FORMAT parquet)
        """
    )


build_user_anchors()
build_active_months()

# %% [markdown]
# ## Проверка

# %%
print(
    con.sql(
        f"""
        WITH anchors AS (
            SELECT
                count(*) AS anchors_n,
                count(first_sub_month) AS with_subscription,
                count(first_paid_month) AS with_paid,
                count(reg_month) AS with_registration
            FROM '{PROCESSED}user_anchors.parquet'
        ),
        active AS (
            SELECT
                count(*) AS active_rows,
                count(DISTINCT msno) AS active_clients,
                min(month) AS first_month,
                max(month) AS last_month
            FROM '{PROCESSED}active_months.parquet'
        )
        SELECT
            anchors_n,
            with_subscription,
            with_paid,
            with_registration,
            active_rows,
            active_clients,
            first_month,
            last_month
        FROM anchors
        CROSS JOIN active
        """
    )
    .df()
    .to_string(index=False)
)
