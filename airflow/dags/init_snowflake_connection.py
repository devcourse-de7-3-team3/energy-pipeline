from airflow import DAG, settings
from airflow.models import Connection
from airflow.decorators import task
from datetime import datetime
import os
import json

with DAG(
    dag_id="init_snowflake_connection",
    description="환경변수 기반 Snowflake 연결 자동 생성",
    start_date=datetime(2025, 11, 13),
    schedule_interval=None,
    catchup=False,
    tags=["setup", "snowflake"],
):

    @task()
    def create_snowflake_connection():
        conn_id = "snowflake_conn"
        session = settings.Session()

        # 이미 존재하면 건너뛰기
        existing_conn = (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .first()
        )
        if existing_conn:
            print(f"⚠️ Connection '{conn_id}' already exists. Skipping.")
            return

        # -------- 환경변수 읽기 --------
        required_envs = [
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_ROLE",
        ]

        missing = [env for env in required_envs if not os.getenv(env)]
        if missing:
            raise ValueError(f"❌ Missing required environment variables: {missing}")

        sf_user = os.getenv("SNOWFLAKE_USER")
        sf_password = os.getenv("SNOWFLAKE_PASSWORD")
        sf_account = os.getenv("SNOWFLAKE_ACCOUNT")  # ex) abcde-xy12345
        sf_database = os.getenv("SNOWFLAKE_DATABASE")
        sf_schema = os.getenv("SNOWFLAKE_SCHEMA")
        sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        sf_role = os.getenv("SNOWFLAKE_ROLE")

        # -------- Host 구성 --------
        # Snowflake Hook은 Host 필드에 "<account>.snowflakecomputing.com" 형태를 기대함
        host = f"{sf_account}.snowflakecomputing.com"

        # -------- Connection 생성 --------
        snowflake_conn = Connection(
            conn_id=conn_id,
            conn_type="snowflake",
            host=host,
            login=sf_user,
            password=sf_password,
            schema=sf_schema,
            extra=json.dumps({
                "account": sf_account,
                "database": sf_database,
                "warehouse": sf_warehouse,
                "role": sf_role,
            }),
        )

        session.add(snowflake_conn)
        session.commit()
        print(f"✅ Snowflake connection '{conn_id}' successfully created!")

    create_snowflake_connection()

