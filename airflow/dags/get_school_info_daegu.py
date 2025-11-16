from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow.models import Variable
import requests


def get_Snowflake_connection(autocommit=True):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn

@task
def call_api(api_url, api_key, atpt_ofcdc_sc_code):
    url = api_url
    params ={'KEY' : api_key,
        'Type' : 'json',
        'pIndex' : '1',
        'pSize' : '1000',
        'ATPT_OFCDC_SC_CODE' : atpt_ofcdc_sc_code}
    response = requests.get(url, params = params).json()
    # 1) 전체 응답 수량 확인
    total_count = int(response['schoolInfo'][0]['head'][0]['list_total_count'])

    # 2) 전체 응답 정보 할당 변수 선언
    all_rows = []

    # 3) 총 페이지 수 계산
    p_size = int(params['pSize'])
    total_pages = (total_count - 1) // p_size + 1

    # 4) 응답 필수정보 담기
    for page in range(1, total_pages + 1):
        params['pIndex'] = str(page)
        res = requests.get(api_url, params=params).json()

        if 'schoolInfo' not in res:
            raise ValueError(f"[ERROR] schoolInfo key not found in API response : {res}")
        
        rows = res['schoolInfo'][1]['row']
        all_rows.extend(rows)

    return all_rows


@task
def extract(records):

    final = []

    for r in records:
        final.append((
            r.get('ATPT_OFCDC_SC_CODE'),
            r.get('ATPT_OFCDC_SC_NM'),
            r.get('SD_SCHUL_CODE'),
            r.get('SCHUL_NM'),
            r.get('ENG_SCHUL_NM'),
            r.get('SCHUL_KND_SC_NM'),
            r.get('LCTN_SC_NM'),
            r.get('JU_ORG_NM'),
            r.get('FOND_SC_NM'),
            r.get('ORG_RDNZC'),
            r.get('ORG_RDNMA'),
            r.get('ORG_RDNDA'),
            r.get('ORG_TELNO'),
            r.get('HMPG_ADRES'),
            r.get('COEDU_SC_NM'),
            r.get('ORG_FAXNO'),
            r.get('HS_SC_NM'),
            r.get('INDST_SPECL_CCCCL_EXST_YN'),
            r.get('HS_GNRL_BUSNS_SC_NM'),
            r.get('SPCY_PURPS_HS_ORD_NM'),
            r.get('ENE_BFE_SEHF_SC_NM'),
            r.get('DGHT_SC_NM'),
            r.get('FOND_YMD'),
            r.get('FOAS_MEMRD'),
            r.get('LOAD_DTM'),
        ))

    return final


def create_table(schema, table):

    conn = get_Snowflake_connection()
    cur = conn.cursor()

    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        ATPT_OFCDC_SC_CODE VARCHAR,
        ATPT_OFCDC_SC_NM   VARCHAR,
        SD_SCHUL_CODE      VARCHAR PRIMARY KEY,
        SCHUL_NM           VARCHAR,
        ENG_SCHUL_NM       VARCHAR,
        SCHUL_KND_SC_NM    VARCHAR,
        LCTN_SC_NM         VARCHAR,
        JU_ORG_NM          VARCHAR,
        FOND_SC_NM         VARCHAR,
        ORG_RDNZC          VARCHAR,
        ORG_RDNMA          VARCHAR,
        ORG_RNDDA          VARCHAR,
        ORG_TELNO          VARCHAR,
        HMPG_ADRES         VARCHAR,
        COEDU_SC_NM        VARCHAR,
        ORG_FAXNO          VARCHAR,
        HS_SC_NM           VARCHAR,
        INDST_SPECL_CCCCL_EXST_YN VARCHAR,
        HS_GNRL_BUSNS_SC_NM VARCHAR,
        SPCY_PURPS_HS_ORD_NM VARCHAR,
        ENE_BFE_SEHF_SC_NM  VARCHAR,
        DGHT_SC_NM          VARCHAR,
        FOND_YMD            VARCHAR,
        FOAS_MEMRD          VARCHAR,
        LOAD_DTM            VARCHAR
    );
    """

    cur.execute(sql)
    cur.close()
    conn.close()


@task
def load(schema, table, records):

    conn = get_Snowflake_connection(autocommit=False)
    cur = conn.cursor()

    schema_only = schema.upper()
    table_only  = table.upper()

    # 테이블 존재 여부 체크
    check_sql = f"SHOW TABLES LIKE '{table_only}' IN SCHEMA {schema_only};"
    cur.execute(check_sql)
    exists = len(cur.fetchall()) > 0

    # 없으면 생성
    if not exists:
        print(f"[INFO] {schema}.{table} not found → creating table...")
        create_table(schema, table)

    # FULL REFRESHMENT 시작
    cur.execute("BEGIN;")

    try:
        # 1) DELETE FROM (TRUNCATE 대신)
        delete_sql = f"DELETE FROM {schema}.{table};"
        cur.execute(delete_sql)

        # 2) INSERT
        insert_sql = f"""
            INSERT INTO {schema}.{table} (
                ATPT_OFCDC_SC_CODE,
                ATPT_OFCDC_SC_NM,
                SD_SCHUL_CODE,
                SCHUL_NM,
                ENG_SCHUL_NM,
                SCHUL_KND_SC_NM,
                LCTN_SC_NM,
                JU_ORG_NM,
                FOND_SC_NM,
                ORG_RDNZC,
                ORG_RDNMA,
                ORG_RNDDA,
                ORG_TELNO,
                HMPG_ADRES,
                COEDU_SC_NM,
                ORG_FAXNO,
                HS_SC_NM,
                INDST_SPECL_CCCCL_EXST_YN,
                HS_GNRL_BUSNS_SC_NM,
                SPCY_PURPS_HS_ORD_NM,
                ENE_BFE_SEHF_SC_NM,
                DGHT_SC_NM,
                FOND_YMD,
                FOAS_MEMRD,
                LOAD_DTM
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            );
        """

        # executemany로 한번에 삽입
        cur.executemany(insert_sql, records)

        # 3) 모든 작업 성공 → COMMIT
        cur.execute("COMMIT;")
        print(f"[SUCCESS] Full refresh committed for {schema}.{table}")
    
    # 에러 발생 시 ROLLBACK
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"[ERROR] Load failed → rollback executed: {str(e)}")
        raise

    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id = 'school_info_DAEGU',
    start_date = datetime(2025, 11, 15),
    catchup = False,
    tags = ['API', 'school_info', 'daegu'],
    schedule = '0 1 * * *'
) as dag:

    API_URL = 'https://open.neis.go.kr/hub/schoolInfo'
    API_KEY = Variable.get("NEIS_KEY")
    ATPT_OFCDC_SC_CODE = 'D10'
    SCHEMA = 'ENERGY_DB.RAW_DATA'
    TABLE = 'SCHOOL_INFO_DAEGU'

    records = call_api(API_URL, API_KEY, ATPT_OFCDC_SC_CODE)
    extracted = extract(records)
    load(SCHEMA, TABLE, extracted)

