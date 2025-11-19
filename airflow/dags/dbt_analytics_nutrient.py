from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "simhoon",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dbt_nutrient_daily",
    default_args=default_args,
    start_date=datetime(2025, 11, 17),
    schedule_interval="0 18 * * *",
    catchup=False,
    tags=["meal", "dbt", "analytics"],
) as dag:

    run_dbt_nutrient = BashOperator(
        task_id="run_dbt_nutrient",
        bash_command=(
            "cd /opt/airflow/meal_dbt && "
            "dbt run -s nutrient"
        ),
    )
