from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow.models import Variable
import requests
import pendulum

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
        ATPT_OFCDC_SC_CODE        VARCHAR COMMENT '시도교육청코드',
        ATPT_OFCDC_SC_NM          VARCHAR COMMENT '시도교육청명',
        SD_SCHUL_CODE             VARCHAR COMMENT '행정표준코드(학교 고유 코드)' PRIMARY KEY,
        SCHUL_NM                  VARCHAR COMMENT '학교명',
        ENG_SCHUL_NM              VARCHAR COMMENT '영문학교명',
        SCHUL_KND_SC_NM           VARCHAR COMMENT '학교종류명,
        LCTN_SC_NM                VARCHAR COMMENT '시도명',
        JU_ORG_NM                 VARCHAR COMMENT '관할조직명',
        FOND_SC_NM                VARCHAR COMMENT '설립명',
        ORG_RDNZC                 VARCHAR COMMENT '도로명우편번호',
        ORG_RDNMA                 VARCHAR COMMENT '도로명주소',
        ORG_RNDDA                 VARCHAR COMMENT '도로명상세주소',
        ORG_TELNO                 VARCHAR COMMENT '전화번호',
        HMPG_ADRES                VARCHAR COMMENT '홈페이지주소',
        COEDU_SC_NM               VARCHAR COMMENT '남녀공학구분명',
        ORG_FAXNO                 VARCHAR COMMENT '팩스번호',
        HS_SC_NM                  VARCHAR COMMENT '고등학교구분명',
        INDST_SPECL_CCCCL_EXST_YN VARCHAR COMMENT '산업체특별학급존재여부',
        HS_GNRL_BUSNS_SC_NM       VARCHAR COMMENT '고등학교일반전문구분명',
        SPCY_PURPS_HS_ORD_NM      VARCHAR COMMENT '특수목적고등학교계열명',
        ENE_BFE_SEHF_SC_NM        VARCHAR COMMENT '입시전후기구분명',
        DGHT_SC_NM                VARCHAR COMMENT '주야구분명',
        FOND_YMD                  VARCHAR COMMENT '설립일자(YYYYMMDD)',
        FOAS_MEMRD                VARCHAR COMMENT '개교기념일',
        LOAD_DTM                  VARCHAR COMMENT '수정일자'
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
    start_date = pendulum.datetime(2025, 10, 1, tz="Asia/Seoul"),
    catchup = False,
    tags = ['API', 'school_info', 'daegu'],
    schedule = '0 1 * * *',
) as dag:

    API_URL = 'https://open.neis.go.kr/hub/schoolInfo'
    API_KEY = Variable.get("NEIS_KEY")
    ATPT_OFCDC_SC_CODE = 'D10'
    SCHEMA = 'ENERGY_DB.RAW_DATA'
    TABLE = 'SCHOOL_INFO_DAEGU'

    records = call_api(API_URL, API_KEY, ATPT_OFCDC_SC_CODE)
    extracted = extract(records)
    load(SCHEMA, TABLE, extracted)

