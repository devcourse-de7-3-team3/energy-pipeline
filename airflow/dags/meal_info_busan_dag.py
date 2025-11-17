from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
import pandas as pd
import time
import json
import concurrent.futures

from snowflake.connector.pandas_tools import write_pandas
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


API_KEY = "1ab95526cf7948ae9cecef4acdd5de39"
BASE_URL_SCHOOL = "https://open.neis.go.kr/hub/schoolInfo"
BASE_URL_MEAL = "https://open.neis.go.kr/hub/mealServiceDietInfo"
ATPT_OFCDC_SC_CODE = "C10"

SNOWFLAKE_CONN_ID = "snowflake_default"
TARGET_SCHEMA = "RAW_DATA"
TARGET_TABLE = "MEAL_DIET_INFO_BUSAN"


default_args = {
    "owner": "airflow",
    "retries": 1,
}


with DAG(
    dag_id="busan_meal_to_snowflake_fast",
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    catchup=True,

    # 🔥 병렬로 5개 날짜 처리 → 5배 빨라짐
    max_active_runs=5,
    max_active_tasks=5,

    default_args=default_args,
    tags=["busan", "meal", "fast", "snowflake"],
):

    # 1) 날짜 계산
    @task
    def get_target_date(**context):
        return context["logical_date"].strftime("%Y%m%d")


    # 2) 학교 목록 1회만 캐싱 (초고속)
    @task
    def fetch_busan_highschools():
        params = {
            "KEY": API_KEY,
            "Type": "json",
            "pSize": 999,
            "ATPT_OFCDC_SC_CODE": ATPT_OFCDC_SC_CODE,
            "SCHUL_KND_SC_NM": "고등학교",
        }

        res = requests.get(BASE_URL_SCHOOL, params=params).json()
        rows = res["schoolInfo"][1]["row"]
        df = pd.DataFrame(rows)[["ATPT_OFCDC_SC_CODE", "SD_SCHUL_CODE", "SCHUL_NM"]]

        return df.to_json(orient="records", force_ascii=False)


    # 3) 멀티스레드 급식 수집 (초고속 버전)
    @task
    def fetch_meal_for_date(school_json: str, target_ymd: str):
        school_df = pd.read_json(school_json)

        def fetch_school(row):
            school_code = row["SD_SCHUL_CODE"]
            school_name = row["SCHUL_NM"]

            params = {
                "KEY": API_KEY,
                "Type": "json",
                "ATPT_OFCDC_SC_CODE": ATPT_OFCDC_SC_CODE,
                "SD_SCHUL_CODE": school_code,
                "MLSV_FROM_YMD": target_ymd,
                "MLSV_TO_YMD": target_ymd,
                "pSize": 100,
            }

            try:
                res = requests.get(BASE_URL_MEAL, params=params).json()
                if "mealServiceDietInfo" not in res:
                    return None
                df = pd.DataFrame(res["mealServiceDietInfo"][1]["row"])
                df["SCHUL_NM"] = school_name
                return df
            except:
                return None

        # 🔥 멀티스레드 → 10~20초면 끝
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(fetch_school, [row for _, row in school_df.iterrows()]))

        valid = [df for df in results if df is not None]

        if not valid:
            return json.dumps([])

        result_df = pd.concat(valid, ignore_index=True)
        return result_df.to_json(orient="records", force_ascii=False)


    # 4) Snowflake 적재
    @task
    def load_to_snowflake(meal_json: str, target_ymd: str):
        df = pd.read_json(meal_json)
        if df.empty:
            print(f"{target_ymd} 데이터 없음 → skip")
            return

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        conn.cursor().execute(f"USE SCHEMA {TARGET_SCHEMA}")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=TARGET_TABLE,
            quote_identifiers=False,
        )

        print(f"Snowflake 적재 완료: {target_ymd}, rows={nrows}")
        conn.close()


    # Pipeline
    target_ymd = get_target_date()
    schools = fetch_busan_highschools()
    meal_json = fetch_meal_for_date(schools, target_ymd)
    load_to_snowflake(meal_json, target_ymd)
