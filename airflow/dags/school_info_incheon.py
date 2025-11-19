from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import requests

# --- 0. 기본 설정 ---

SNOWFLAKE_CONN_ID = "snowflake_conn"
TARGET_DATABASE = "ENERGY_DB"
TARGET_SCHEMA = "RAW_DATA"
TARGET_TABLE = "SCHOOL_INFO_INCHEON"

BASE_URL = "https://open.neis.go.kr/hub/schoolInfo"
# 인천광역시교육청(E10), 고등학교 대상
ATPT_OFCDC_SC_CODE = "E10"
SCHUL_KND_SC_NM = "고등학교"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# --- 1. DAG 생성 ---

with DAG(
    dag_id="school_info_incheon",
    start_date=datetime(2025, 11, 16),
    schedule="0 16 * * *",
    catchup=False,
    default_args=default_args,
    tags=["API", "school_info", "incheon", "snowflake"],
) as dag:

# --- 2. 인천 고등학교 기본정보 fetch 태스크 ---
    @task()
    def fetch_school_info_incheon():
        """
        - NEIS 학교기본정보 API에서 인천 고등학교 전체 리스트 요청
        - 응답을 [{}, {}, ...] 형태로 반환.
        """
        try:
            api_key = Variable.get("NEIS_KEY")
            params = {
                "KEY": api_key,
                "Type": "json",
                "pIndex": 1,
                "pSize": 200,
                "ATPT_OFCDC_SC_CODE": ATPT_OFCDC_SC_CODE,
                "SCHUL_KND_SC_NM": SCHUL_KND_SC_NM,
            }

            # API 요청
            resp = requests.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()  # HTTP 오류 시 즉시 예외 발생

            # JSON 파싱
            data = resp.json()

            # 구조 확인
            school_info = data.get("schoolInfo")
            if not school_info or len(school_info) < 2:
                raise ValueError("NEIS API 구조가 예상과 다름. 응답 구조 확인 필요.")

            rows = school_info[1].get("row", [])
            print(f"총 {len(rows)}개 학교 정보 수집 완료")
            return rows

        except Exception as e:
            raise RuntimeError(f"[학교정보 API 오류] 인천 고등학교 정보 수집 실패: {e}")

    
# --- 3. snoflake 적재 태스크 ---
    @task()
    def load_to_snowflake(rows):
        """
        - rows가 비어 있으면 아무 작업도 하지 않음
        - rows[0]의 키를 기준으로 테이블 스키마 생성 (모든 컬럼 VARCHAR)
        - full refresh
        """
        if not rows:
            print("가져온 데이터 없음. Snowflake 적재 X")
            return

        try:
            # 1) 컬럼명은 첫 번째 row의 키에서 가져옴
            columns = list(rows[0].keys())

            # Snowflake 연결
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            conn = hook.get_conn()
            cursor = conn.cursor()

            # 2) CREATE TABLE (모든 컬럼 VARCHAR)
            full_table_name = f'{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}'
            cols_ddl = ", ".join([f'"{col}" VARCHAR' for col in columns])
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {cols_ddl}
            )
            """
            print("CREATE TABLE IF NOT EXISTS 실행...")
            cursor.execute(create_sql)

            # 3) TRUNCATE (full refresh)
            truncate_sql = f"TRUNCATE TABLE {full_table_name}"
            print("TRUNCATE TABLE 실행...")
            cursor.execute(truncate_sql)

            # 4) INSERT
            col_list = ", ".join([f'"{col}"' for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"""
            INSERT INTO {full_table_name} ({col_list})
            VALUES ({placeholders})
            """

            values = []
            for r in rows:
                values.append(tuple(r.get(col) for col in columns))

            print(f"{len(values)}건 INSERT 실행...")
            cursor.executemany(insert_sql, values)
            conn.commit()

            cursor.close()
            conn.close()
            print(f"Snowflake 적재 완료: {full_table_name}")

        except Exception as e:
            raise RuntimeError(f"[Snowflake 적재 오류] SCHOOL_INFO_INCHEON 적재 실패: {e}")


# --- 4. 태스크 의존성 설정 ---
    fetched_rows = fetch_school_info_incheon()
    load_to_snowflake(fetched_rows)
