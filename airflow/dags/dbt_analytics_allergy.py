from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "jaemin",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dbt_analytics_daily",          
    default_args=default_args,
    start_date=datetime(2025, 11, 17),      
    schedule_interval="0 18 * * *",          # 매일 03:00 실행 (stg 끝난 후 실행)
    catchup=False,
    tags=["meal", "dbt", "analytics"],
) as dag:

    run_dbt_analytics = BashOperator(
        task_id="run_dbt_analytics",
        bash_command=(
            "cd /opt/airflow/meal_dbt && "
            "dbt run -s analytics"          # analytics 폴더만 실행
        ),
    )   
