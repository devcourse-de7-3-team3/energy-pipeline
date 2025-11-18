from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import requests
import urllib.parse # ServiceKey 디코딩을 위한 모듈 추가
import requests
import pandas as pd
import json
import logging
import psycopg2
from datetime import datetime, date, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import snowflake.connector
from pendulum import timezone


def get_snowflake_cursor(snowflake_conn_id: str='snowflake_conn') -> snowflake.connector.cursor.SnowflakeCursor:
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    conn = hook.get_conn()
    return conn.cursor()


def get_school_info(school_data):
    extracted_data = []
    Target_fields = [
        'ATPT_OFCDC_SC_CODE', 'ATPT_OFCDC_SC_NM', 'SD_SCHUL_CODE', 'SCHUL_NM', 'ENG_SCHUL_NM',
        'SCHUL_KND_SC_NM', 'LCTN_SC_NM', 'JU_ORG_NM', 'FOND_SC_NM', 'ORG_RDNZC', 'ORG_RDNMA',
        'ORG_RDNDA', 'ORG_TELNO', 'HMPG_ADRES', 'COEDU_SC_NM', 'ORG_FAXNO', 'HS_SC_NM', 'INDST_SPECL_CCCCL_EXST_YN',
        'HS_GNRL_BUSNS_SC_NM', 'SPCLY_PURPS_HS_ORD_NM', 'ENE_BFE_SEHF_SC_NM', 'DGHT_SC_NM', 'FOND_YMD',
        'FOAS_MEMRD', 'LOAD_DTM'
    ]
    
    diet_info = school_data.get("schoolInfo")
    
    if isinstance(diet_info[1], dict) and "row" in diet_info[1]:
        rows = diet_info[1]["row"]
    
    for item in rows:
        extracted_row={}
        for key in Target_fields:
            extracted_row[key] = item.get(key, "N/A")
        extracted_data.append(extracted_row)
    
    return extracted_data


@task
def extract_transform():
    BASE_URL = "https://open.neis.go.kr/hub/schoolInfo"

    DECODED_SERVICE_KEY = Variable.get("NEIS_KEY_school")
    school_info=[]
    
    parameters = {
        "Key": DECODED_SERVICE_KEY, 
        "Type": 'json',
        "pindex": 1,
        "pSize": 340,
        "ATPT_OFCDC_SC_CODE": 'F10',
        "LCTN_SC_NM": '광주광역시'   
    }
    
    response = requests.get(BASE_URL, params=parameters)
    if response.status_code == 200:
        data = response.json()
        print('--data확인----------', data)
        school_info = get_school_info(data)
        school_info.append(data)
    return school_info


@task
def load(schema, table, records):
    logging.info("------ load started ------")
        
    curr = get_snowflake_cursor()
    
    if len(records) == 0:
        logging.info("No records to load. Skipping.")
        return
    
    try:
        curr.execute("BEGIN;")
        curr.execute(f"DELETE FROM {schema}.{table};")      # full refresh

        logging.info(f"Deleting existing data of {schema}.{table}")     
                
        sql_template = """
            INSERT INTO {schema}.{table} (
                ATPT_OFCDC_SC_CODE, ATPT_OFCDC_SC_NM, SD_SCHUL_CODE, SCHUL_NM, ENG_SCHUL_NM,
                SCHUL_KND_SC_NM, LCTN_SC_NM, JU_ORG_NM, FOND_SC_NM, ORG_RDNZC, ORG_RDNMA,
                ORG_RDNDA, ORG_TELNO, HMPG_ADRES, COEDU_SC_NM, ORG_FAXNO, HS_SC_NM, INDST_SPECL_CCCCL_EXST_YN,
                HS_GNRL_BUSNS_SC_NM, SPCLY_PURPS_HS_ORD_NM, ENE_BFE_SEHF_SC_NM, DGHT_SC_NM, FOND_YMD,
                FOAS_MEMRD, LOAD_DTM
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s             
            );
        """
        requried_keys = ['ATPT_OFCDC_SC_CODE', 'ATPT_OFCDC_SC_NM', 'SD_SCHUL_CODE', 'SCHUL_NM', 'ENG_SCHUL_NM',
                'SCHUL_KND_SC_NM', 'LCTN_SC_NM', 'JU_ORG_NM', 'FOND_SC_NM', 'ORG_RDNZC', 'ORG_RDNMA',
                'ORG_RDNDA', 'ORG_TELNO', 'HMPG_ADRES', 'COEDU_SC_NM', 'ORG_FAXNO', 'HS_SC_NM', 'INDST_SPECL_CCCCL_EXST_YN',
                'HS_GNRL_BUSNS_SC_NM', 'SPCLY_PURPS_HS_ORD_NM', 'ENE_BFE_SEHF_SC_NM', 'DGHT_SC_NM', 'FOND_YMD',
                'FOAS_MEMRD', 'LOAD_DTM']
        
        formatted_sql = sql_template.format(schema=schema, table=table)
        
        for record in records:
            if record.get('SCHUL_KND_SC_NM') == '고등학교':
                print("기록될 정보: ",record)
                data_to_insert = tuple(record.get(key) for key in requried_keys)
                curr.execute(formatted_sql, data_to_insert)
            
        curr.execute("COMMIT;")
            
        logging.info(f"Successfully loaded {len(records)}")
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error during load: {error}")
        curr.execute("ROLLBACK;")
        raise
    logging.info("load done")


seoul = timezone("Asia/Seoul")

with DAG(
    dag_id='SCHOOL_INFO_GWANGJU',
    start_date=datetime(2025,11,17, tzinfo=seoul), # dag가 실행되어야할 가장 이른 날짜
    schedule='0 0 * * *',
    max_active_runs=1,
    catchup=True,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    schema = 'raw_data'
    table = 'SCHOOL_INFO_GWANGJU'
    lines = extract_transform()    # dag run이 스케줄된 날짜
    load(schema, table, lines)