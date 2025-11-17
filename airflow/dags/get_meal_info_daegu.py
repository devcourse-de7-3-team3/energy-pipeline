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
def get_code_daegu():
    conn = get_Snowflake_connection()
    cur = conn.cursor()
    sql = """
        SELECT DISTINCT atpt_ofcdc_sc_code, sd_schul_code
        FROM ENERGY_DB.RAW_DATA.SCHOOL_INFO_DAEGU
        WHERE SCHUL_NM like '%고등학교%';
    """
    cur.execute(sql)
    code_list = [(r[0], r[1]) for r in cur.fetchall()]

    cur.close()
    return code_list


@task
def call_api(api_url, api_key, code_list, logical_date=None):

    url = api_url
    req_date = logical_date.strftime("%Y%m%d")

    all_rows = []

    for atpt_code, schul_code in code_list:

        params ={'KEY' : api_key,
            'Type' : 'json',
            'pIndex' : '1',
            'pSize' : '1000',
            'ATPT_OFCDC_SC_CODE' : atpt_code, 
            'SD_SCHUL_CODE' : schul_code,
            'MLSV_YMD' : req_date
            }
        res = requests.get(url, params = params).json()

        if "mealServiceDietInfo" not in res:
            # 날짜에 급식이 없으면 PASS (에러 아님)
            print(f"[WARN] 급식 정보 없음 — 학교:{schul_code}, 날짜:{req_date}")
            continue

        rows = res['mealServiceDietInfo'][1]['row']
        all_rows.extend(rows)

    return all_rows

@task
def extract(records):

    final = []

    for r in records:
        final.append((
            r.get("ATPT_OFCDC_SC_CODE"),
            r.get("ATPT_OFCDC_SC_NM"),
            r.get("SD_SCHUL_CODE"),
            r.get("SCHUL_NM"),
            r.get("MMEAL_SC_CODE"),
            r.get("MMEAL_SC_NM"),
            r.get("MLSV_YMD"),
            r.get("MLSV_FGR"),
            r.get("DDISH_NM"),
            r.get("ORPLC_INFO"),
            r.get("CAL_INFO"),
            r.get("NTR_INFO"),
            r.get("MLSV_FROM_YMD"),
            r.get("MLSV_TO_YMD"),
            r.get("LOAD_DTM")
        ))

    return final


def create_table(schema, table):

    conn = get_Snowflake_connection()
    cur = conn.cursor()

    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        ATPT_OFCDC_SC_CODE   VARCHAR COMMENT '시도교육청코드',
        ATPT_OFCDC_SC_NM     VARCHAR COMMENT '시도교육청명',
        SD_SCHUL_CODE        VARCHAR COMMENT '행정표준코드(학교 고유코드)',
        SCHUL_NM             VARCHAR COMMENT '학교명',
        MMEAL_SC_CODE        VARCHAR COMMENT '식사코드(1:조식, 2:중식, 3:석식)',
        MMEAL_SC_NM          VARCHAR COMMENT '식사명(조식/중식/석식)',
        MLSV_YMD             VARCHAR COMMENT '급식일자(YYYYMMDD)',
        MLSV_FGR             VARCHAR COMMENT '급식인원수',
        DDISH_NM             VARCHAR COMMENT '요리명',
        ORPLC_INFO           VARCHAR COMMENT '원산지정보',
        CAL_INFO             VARCHAR COMMENT '칼로리정보',
        NTR_INFO             VARCHAR COMMENT '영양정보',
        MLSV_FROM_YMD        VARCHAR COMMENT '급식시작일자',
        MLSV_TO_YMD          VARCHAR COMMENT '급식종료일자',
        LOAD_DTM             VARCHAR COMMENT '수정일자(데이터 업데이트 일시)',
        CONSTRAINT PK_MEAL_INFO_DAEGU
            PRIMARY KEY (ATPT_OFCDC_SC_CODE, SD_SCHUL_CODE, MMEAL_SC_CODE, MLSV_YMD)
    );
    """

    cur.execute(sql)
    cur.close()


@task
def load(schema, table, records):

    conn = get_Snowflake_connection(autocommit=False)
    cur = conn.cursor()

    # 테이블 존재 여부 확인
    cur.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA {schema};")
    exists = len(cur.fetchall()) > 0

    if not exists:
        print(f"[INFO] {schema}.{table} not found → creating table...")
        create_table(schema, table)

    # MERGE SQL
    merge_sql = f"""
        MERGE INTO {schema}.{table} AS T
        USING (
            SELECT 
                %s AS ATPT_OFCDC_SC_CODE,
                %s AS ATPT_OFCDC_SC_NM,
                %s AS SD_SCHUL_CODE,
                %s AS SCHUL_NM,
                %s AS MMEAL_SC_CODE,
                %s AS MMEAL_SC_NM,
                %s AS MLSV_YMD,
                %s AS MLSV_FGR,
                %s AS DDISH_NM,
                %s AS ORPLC_INFO,
                %s AS CAL_INFO,
                %s AS NTR_INFO,
                %s AS MLSV_FROM_YMD,
                %s AS MLSV_TO_YMD,
                %s AS LOAD_DTM
        ) AS S
        ON  T.SD_SCHUL_CODE = S.SD_SCHUL_CODE
        AND T.MMEAL_SC_CODE = S.MMEAL_SC_CODE
        AND T.MLSV_YMD = S.MLSV_YMD
        
        WHEN MATCHED THEN UPDATE SET
            ATPT_OFCDC_SC_CODE = S.ATPT_OFCDC_SC_CODE,
            ATPT_OFCDC_SC_NM   = S.ATPT_OFCDC_SC_NM,
            SCHUL_NM           = S.SCHUL_NM,
            MMEAL_SC_NM        = S.MMEAL_SC_NM,
            MLSV_FGR           = S.MLSV_FGR,
            DDISH_NM           = S.DDISH_NM,
            ORPLC_INFO         = S.ORPLC_INFO,
            CAL_INFO           = S.CAL_INFO,
            NTR_INFO           = S.NTR_INFO,
            MLSV_FROM_YMD      = S.MLSV_FROM_YMD,
            MLSV_TO_YMD        = S.MLSV_TO_YMD,
            LOAD_DTM           = S.LOAD_DTM

        WHEN NOT MATCHED THEN INSERT VALUES (
            S.ATPT_OFCDC_SC_CODE, S.ATPT_OFCDC_SC_NM, S.SD_SCHUL_CODE, S.SCHUL_NM,
            S.MMEAL_SC_CODE, S.MMEAL_SC_NM, S.MLSV_YMD, S.MLSV_FGR, S.DDISH_NM,
            S.ORPLC_INFO, S.CAL_INFO, S.NTR_INFO, S.MLSV_FROM_YMD, S.MLSV_TO_YMD,
            S.LOAD_DTM
        );
    """

    # MERGE 실행
    try:
        for rec in records:
            cur.execute(merge_sql, rec)

        cur.execute("COMMIT;")
        print(f"[SUCCESS] Incremental load successful for {schema}.{table}")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"[ERROR] Incremental load failed → rollback executed: {str(e)}")
        raise

    finally:
        cur.close()


with DAG(
    dag_id = 'meal_info_DAEGU',
    start_date = pendulum.datetime(2025, 10, 1, tz="Asia/Seoul"),
    catchup = True,
    tags = ['API', 'meal_info', 'daegu'],
    schedule = '10 1 * * *',
) as dag:

    API_URL = 'https://open.neis.go.kr/hub/mealServiceDietInfo'
    API_KEY = Variable.get("NEIS_KEY")
    SCHEMA = 'ENERGY_DB.RAW_DATA'
    TABLE = 'MEAL_DIET_INFO_DAEGU'

    codes = get_code_daegu()
    records = call_api(API_URL, API_KEY, codes)
    extracted = extract(records)
    load(SCHEMA, TABLE, extracted)
