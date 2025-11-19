from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "yewon",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dbt_run_gwangju_school_meal",
    default_args = default_args,
    start_date=datetime(2025, 11, 15),
    schedule_interval="0 18 * * *",
    catchup=False
):

    # wait_for_prev_dag = ExternalTaskSensor(
    #     task_id="wait_for__stg_meal_diet_all",
    #     external_dag_id="dbt_stg_meal_diet_info_all_daily",  
    #     external_task_id="run_dbt_stg_meal_diet_info_all",   # DAG 전체가 끝날 때까지 기다림
    #     allowed_states=["success"],
    #     failed_states=["failed", "skipped"],
    #     poke_interval=3600,        # 1시간 마다 상태 체크
    #     timeout=3600 * 6         # 6시간 기다림
    # )
    
    dbt_run_gwanju_school_meal = BashOperator(
        task_id="dbt_run_gwangju_school_meal",
        bash_command="cd /opt/airflow/meal_dbt/models/analytics && dbt run -s inter_menu"
    )
