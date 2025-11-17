from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "simhoon",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dbt_stg_meal_diet_info_all_daily",
    default_args=default_args,
    start_date=datetime(2025, 11, 17),      
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["meal", "dbt", "staging"],
) as dag:


    run_dbt_stg_meal_diet_info_all = BashOperator(
        task_id="run_dbt_stg_meal_diet_info_all",
        bash_command=(
            "cd /opt/airflow/meal_dbt && "
            "dbt run -s stg_meal_diet_info_all"
        ),
    )
