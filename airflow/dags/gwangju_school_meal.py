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


def get_snowflake_cursor(snowflake_conn_id: str='snowflake_conn') -> snowflake.connector.cursor.SnowflakeCursor:
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    conn = hook.get_conn()

    return conn.cursor()

@task
def get_school_codes_snowflake(table_name, snowflake_conn_id: str='snowflake_conn'):
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    sql_query = f"""
        SELECT 
            DISTINCT SD_SCHUL_CODE 
        FROM 
            {table_name};
    """
    records = hook.get_records(sql=sql_query)
    print("snowflake에서 불러온 record 수", len(records))
    school_code_list = [record[0] for record in records]
    return school_code_list     # xcom으로 처리

def get_meal_info(school_info):
    extracted_data = []
    Target_fields = [
        "ATPT_OFCDC_SC_CODE",
        "ATPT_OFCDC_SC_NM",
        "SD_SCHUL_CODE",
        "SCHUL_NM",
        "MMEAL_SC_CODE",
        "MMEAL_SC_NM",
        "MLSV_YMD",
        "MLSV_FGR",
        "DDISH_NM",
        "ORPLC_INFO",
        "CAL_INFO",
        "NTR_INFO",
        "MLSV_FROM_YMD",
        "MLSV_TO_YMD",
        "LOAD_DTM"
    ]
    
    diet_info = school_info.get("mealServiceDietInfo")
    
    if diet_info is None or not isinstance(diet_info, list) or len(diet_info) < 2:
        return extracted_data       #empty
    
    if isinstance(diet_info[1], dict) and "row" in diet_info[1]:
        rows = diet_info[1]["row"]
    else:
        return extracted_data       #empty
    
    for item in rows:
        extracted_row={}
        for key in Target_fields:
            extracted_row[key] = item.get(key, "N/A")
        extracted_data.append(extracted_row)
    
    return extracted_data

@task
def extract_transform(school_code_list, **context):
    BASE_URL = "https://open.neis.go.kr/hub/mealServiceDietInfo"

    DECODED_SERVICE_KEY = Variable.get("NEIS_KEY_meal")
    print("전체 학교 수 ", len(school_code_list))
    school_meal=[]
    execution_date = context.get('ds')
    
    if execution_date:
        execution_dt = datetime.strptime(execution_date, '%Y-%m-%d')
        target_date_str = execution_dt.strftime('%Y%m%d')        
        logging.info(f"[Execution date(raw)] = {execution_date}")
        logging.info(f"[Execution date(parsed)] = {execution_dt}")
        logging.info(f"[Target date] = {target_date_str}")

        nodate = ['20250930']
        if target_date_str in nodate:
            return []
        
    else:
        logging.error("execution_date가 전달되지 않음")
        return []
    cnt=1
    for code in school_code_list:           ##### 자주 건드리는 반복문!!!!! 확인!!!!! ##################
        parameters = {
            "Key": DECODED_SERVICE_KEY, 
            "Type": 'json',
            "pindex": 1,
            "pSize": 100,
            "ATPT_OFCDC_SC_CODE": 'F10',
            "SD_SCHUL_CODE": code,
            "MLSV_FROM_YMD": target_date_str,
            "MLSV_TO_YMD":target_date_str
        }

        logging.info(f"추출 중인 학교: {code} 순서: {cnt}/71")
        cnt += 1
        try:
            response = requests.get(BASE_URL, params=parameters)    # 학교 하나, 날짜 하나

            if response.status_code == 200:
                data = response.json()
                print('--data확인----------', data)
                meal_info = get_meal_info(data)     # A학교의 20251001 급식 정보
                if len(meal_info)>0:
                    school_meal.extend(meal_info)   # 존재하면 추가
                
        except requests.exceptions.RequestException as e:
            print(f"요청 중 오류 발생: {e}")
    
    print("수집된 학교 급식 수 (school meal)", len(school_meal))
    return school_meal


@task
def load(schema, table, records, **context):
    logging.info("------ load started ------")
    execution_date = context.get('ds')
    
    execution_dt = datetime.strptime(execution_date, '%Y-%m-%d')
    target_date_str = execution_dt.strftime('%Y%m%d')
    
    curr = get_snowflake_cursor()
    
    if len(records) == 0:
        logging.info("No records to load. Skipping.")
        return
    
    try:
        curr.execute("BEGIN;")

        delete_sql = f"""
            DELETE FROM {schema}.{table}
            WHERE MLSV_YMD = TO_DATE('{target_date_str}', 'YYYYMMDD');
        """
        logging.info(f"Deleting existing data for {target_date_str}")     
        curr.execute(delete_sql)
                
        sql_template = """
            INSERT INTO {schema}.{table} (
                ATPT_OFCDC_SC_CODE, ATPT_OFCDC_SC_NM, SD_SCHUL_CODE, SCHUL_NM, MMEAL_SC_CODE,
                MMEAL_SC_NM, MLSV_YMD, MLSV_FGR, DDISH_NM, ORPLC_INFO, CAL_INFO, NTR_INFO,
                MLSV_FROM_YMD, MLSV_TO_YMD, LOAD_DTM
            ) VALUES (
                %s, %s, %s, %s, %s, %s, TO_DATE(%s, 'YYYYMMDD'), %s, %s, %s,
                %s, %s, TO_DATE(%s, 'YYYYMMDD'), TO_DATE(%s, 'YYYYMMDD'), TO_DATE(%s, 'YYYYMMDD')
            );
        """
        requried_keys = ['ATPT_OFCDC_SC_CODE', 'ATPT_OFCDC_SC_NM', 'SD_SCHUL_CODE', 'SCHUL_NM', 'MMEAL_SC_CODE',
                    'MMEAL_SC_NM', 'MLSV_YMD', 'MLSV_FGR', 'DDISH_NM', 'ORPLC_INFO', 'CAL_INFO', 'NTR_INFO',
                    'MLSV_FROM_YMD', 'MLSV_TO_YMD', 'LOAD_DTM']     # 15개
        
        formatted_sql = sql_template.format(schema=schema, table=table)
        
        for record in records:
            print("기록될 정보: ",record) 
            data_to_insert = tuple(record.get(key) for key in requried_keys)
            curr.execute(formatted_sql, data_to_insert)
            
        curr.execute("COMMIT;")
            
        logging.info(f"Successfully loaded {len(records)} records for {target_date_str}")
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error during load: {error}")
        curr.execute("ROLLBACK;")
        raise
    logging.info("load done")

with DAG(
    dag_id='MEAL_DIET_INFO_Gwangju',
    start_date=datetime(2025,10,1), # dag가 실행되어야할 가장 이른 날짜
    schedule='0 1 * * *',
    max_active_runs=1,
    catchup=True,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    schema = 'raw_data'
    table = 'MEAL_DIET_INFO_GWANGJU'
    school_codes = get_school_codes_snowflake('SCHOOL_INFO_GWANGJU', snowflake_conn_id='snowflake_conn')
    lines = extract_transform(school_codes)    # dag run이 스케줄된 날짜
    load(schema, table, lines)