from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta, date
import clickhouse_connect
import os

# Настройки ClickHouse из переменных окружения (заданы в docker-compose)
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "reports_mart")

# ID соединений в Airflow (создаются в UI)
POSTGRES_CRM_CONN = "postgres_crm"
POSTGRES_TELEMETRY_CONN = "postgres_db"

default_args = {
    "owner": "bionicpro",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="report_mart_daily",
    default_args=default_args,
    schedule="0 2 * * *",  # Каждый день в 02:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["reports", "etl"],
) as dag:

    @task
    def create_mart_table():
        """Создает таблицу витрины в ClickHouse, если её нет."""
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
        )
        client.command("""
            CREATE TABLE IF NOT EXISTS prosthesis_usage_daily
            (
                user_id String,
                prosthesis_id String,
                report_date Date,
                total_signals UInt64,
                recognized_moves UInt64,
                avg_response_ms Float32,
                error_count UInt32
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(report_date)
            ORDER BY (user_id, report_date, prosthesis_id)
        """)
        client.close()

    @task
    def extract_and_load():
        """Извлекает данные из CRM и Телеметрии, объединяет и грузит в ClickHouse."""
        from datetime import date

        # 1. CRM: получаем связку пользователь -> протез
        crm_hook = PostgresHook(postgres_conn_id=POSTGRES_CRM_CONN)
        crm_rows = crm_hook.get_records(
            sql="""
                SELECT user_id, prosthesis_id
                FROM crm_user_prosthesis
                WHERE is_active = true
            """
        )

        # 2. Телеметрия: агрегируем данные за последние 24 часа (вместо привязки к ds)
        telem_hook = PostgresHook(postgres_conn_id=POSTGRES_TELEMETRY_CONN)
        telem_rows = telem_hook.get_records(
            sql="""
                SELECT prosthesis_id,
                       COUNT(*) AS total_signals,
                       SUM(CASE WHEN recognized THEN 1 ELSE 0 END) AS recognized_moves,
                       AVG(response_ms) AS avg_response_ms,
                       SUM(CASE WHEN error THEN 1 ELSE 0 END) AS error_count
                FROM telemetry_signals
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY prosthesis_id
            """
        )

        # 3. Объединение данных в Python
        telem_map = {r[0]: r[1:] for r in telem_rows}
        rows_to_insert = []

        # В витрину пишем сегодняшнюю дату
        today = date.today()

        for user_id, prosthesis_id in crm_rows:
            t = telem_map.get(prosthesis_id)
            if t:
                rows_to_insert.append((
                    user_id, prosthesis_id, today,
                    t[0], t[1], t[2] or 0.0, t[3] or 0,
                ))

        # 4. Загрузка в ClickHouse
        if rows_to_insert:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DB,
            )
            client.insert(
                table="prosthesis_usage_daily",
                data=rows_to_insert,
                column_names=[
                    "user_id", "prosthesis_id", "report_date",
                    "total_signals", "recognized_moves",
                    "avg_response_ms", "error_count",
                ],
            )
            client.close()

        return f"Inserted {len(rows_to_insert)} rows"

    # Определяем порядок выполнения задач
    create_task = create_mart_table()
    load_task = extract_and_load()

    create_task >> load_task
