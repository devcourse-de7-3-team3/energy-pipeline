from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "jungeun",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dbt_calorie_daily",
    default_args=default_args,
    start_date=datetime(2025, 11, 17),      
    schedule_interval="0 18 * * *",
    catchup=False,
    tags=["calorie", "dbt", "analytics"],
) as dag:


    run_dbt_calorie = BashOperator(
        task_id="run_dbt_calorie",
        bash_command=(
            "cd /opt/airflow/meal_dbt && "
            "dbt run -s calorie"
        ),
    )
