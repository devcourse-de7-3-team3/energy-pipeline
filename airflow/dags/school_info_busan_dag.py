from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

API_KEY = "1ab95526cf7948ae9cecef4acdd5de39"
BASE_URL = "https://open.neis.go.kr/hub/schoolInfo"


def fetch_and_load(**context):

    # 1) API 호출
    params = {
        "KEY": API_KEY,
        "Type": "json",
        "pIndex": 1,
        "pSize": 100,
        "ATPT_OFCDC_SC_CODE": "C10",
        "SCHUL_KND_SC_NM": "고등학교"
    }

    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if "schoolInfo" not in data:
        print("데이터 없음")
        return

    rows = data["schoolInfo"][1]["row"]
    df = pd.DataFrame(rows)

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # 2) 테이블 생성
    create_table_sql = """
    CREATE OR REPLACE TABLE SCHOOL_INFO_BUSAN (
        ATPT_OFCDC_SC_CODE STRING,
        ATPT_OFCDC_SC_NM STRING,
        SD_SCHUL_CODE STRING,
        SCHUL_NM STRING,
        ENG_SCHUL_NM STRING,
        SCHUL_KND_SC_NM STRING,
        LCTN_SC_NM STRING,
        JU_ORG_NM STRING,
        FOND_SC_NM STRING,
        ORG_RDNZC STRING,
        ORG_RDNMA STRING,
        ORG_RDNDA STRING,
        ORG_TELNO STRING,
        HMPG_ADRES STRING,
        COEDU_SC_NM STRING,
        ORG_FAXNO STRING,
        HS_SC_NM STRING,
        INDST_SPECL_CCCCL_EXST_YN STRING,
        HS_GNRL_BUSNS_SC_NM STRING,
        SPCLY_PURPS_HS_ORD_NM STRING,
        ENE_BFE_SEHF_SC_NM STRING,
        DGHT_SC_NM STRING,
        FOND_YMD STRING,
        FOAS_MEMRD STRING,
        LOAD_DTM STRING
    );
    """
    cursor.execute(create_table_sql)

    # 3) INSERT
    insert_sql = """
    INSERT INTO SCHOOL_INFO_BUSAN (
        ATPT_OFCDC_SC_CODE, ATPT_OFCDC_SC_NM, SD_SCHUL_CODE, SCHUL_NM,
        ENG_SCHUL_NM, SCHUL_KND_SC_NM, LCTN_SC_NM, JU_ORG_NM, FOND_SC_NM,
        ORG_RDNZC, ORG_RDNMA, ORG_RDNDA, ORG_TELNO, HMPG_ADRES, COEDU_SC_NM,
        ORG_FAXNO, HS_SC_NM, INDST_SPECL_CCCCL_EXST_YN, HS_GNRL_BUSNS_SC_NM,
        SPCLY_PURPS_HS_ORD_NM, ENE_BFE_SEHF_SC_NM, DGHT_SC_NM, FOND_YMD,
        FOAS_MEMRD, LOAD_DTM
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows_to_insert = [tuple(row.values) for _, row in df.iterrows()]
    cursor.executemany(insert_sql, rows_to_insert)

    conn.commit()
    cursor.close()
    conn.close()


default_args = {
    "owner": "airflow"
}

dag = DAG(
    dag_id="SCHOOL_INFO_BUSAN_DAG",
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,      
    catchup=False,               
    default_args=default_args
)

task = PythonOperator(
    task_id="fetch_school_info_busan",
    python_callable=fetch_and_load,
    dag=dag,
)
