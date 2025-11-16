from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2025, 11, 13),
    schedule_interval=None,
    catchup=False,
    tags=["test", "snowflake"]
):

    @task
    def test_conn():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

        # DB 연결
        conn = hook.get_conn()
        cursor = conn.cursor()

        # 현재 시각 반환 테스트
        cursor.execute("SELECT CURRENT_TIMESTAMP();")
        result = cursor.fetchone()
        print(f"✅ Snowflake connected successfully: {result}")
        cursor.close()
        conn.close()

    test_conn()

