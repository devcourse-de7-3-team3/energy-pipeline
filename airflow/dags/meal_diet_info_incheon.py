from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import get_current_context

from datetime import datetime, timedelta
import requests

# --- 0. 기본 설정 ---

SNOWFLAKE_CONN_ID = "snowflake_conn"
TARGET_DATABASE = "ENERGY_DB"
TARGET_SCHEMA = "RAW_DATA"
TARGET_TABLE = "MEAL_DIET_INFO_INCHEON"
BASE_URL = "https://open.neis.go.kr/hub/mealServiceDietInfo"

# 인천 학교 기본정보 테이블
SCHOOL_INFO_TABLE = "ENERGY_DB.RAW_DATA.SCHOOL_INFO_INCHEON"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# --- 1. DAG 생성 ---
# catchup=True 이므로, start_date~현재까지 날짜별로 한 번씩 backfill 가능

with DAG(
    dag_id="meal_diet_info_incheon",
    start_date=datetime(2025, 10, 1),
    schedule="10 16 * * *",
    catchup=True,
    default_args=default_args,
    tags=["API", "meal_info", "incheon", "snowflake"],
) as dag:

    # --- 2. 인천 고등학교 코드 목록 가져오는 태스크 ---
    @task()
    def get_school_codes():
        """
        SCHOOL_INFO_INCHEON에서 ATPT_OFCDC_SC_CODE, SD_SCHUL_CODE 목록 조회
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = f"""
        SELECT DISTINCT "ATPT_OFCDC_SC_CODE", "SD_SCHUL_CODE"
        FROM {SCHOOL_INFO_TABLE}
        """
        cursor.execute(sql)
        rows = cursor.fetchall()

        if not rows:
            cursor.close()
            conn.close()
            raise RuntimeError("[학교목록 조회 오류] SCHOOL_INFO_INCHEON에 데이터 없음")

        code_list = [(r[0], r[1]) for r in rows]
        print(f"총 {len(code_list)}개 학교 코드 조회 완료")

        cursor.close()
        conn.close()
        return code_list

    # --- 3. logical_date 기준으로 하루치 급식 API 호출 태스크 ---
    @task()
    def fetch_meal_for_date(api_url: str, api_key: str, code_list: list):
        """
        - Airflow logical_date(실행 시점의 데이터 날짜)를 기준으로
          해당 날짜의 급식 정보를 모든 학교에 대해 호출
        - MLSV_YMD = logical_date (YYYYMMDD)
        - 주말/방학 등 급식 없는 날은 0건 반환 (정상)
        """
        # Airflow 컨텍스트에서 logical_date 가져오기
        context = get_current_context()
        logical_date = context["logical_date"]
        req_date = logical_date.date().strftime("%Y%m%d")

        print(f"[INFO] 급식 조회 날짜: {req_date}")

        all_rows = []

        for atpt_code, schul_code in code_list:
            try:
                params = {
                    "KEY": api_key,
                    "Type": "json",
                    "pIndex": 1,
                    "pSize": 1000,
                    "ATPT_OFCDC_SC_CODE": atpt_code,
                    "SD_SCHUL_CODE": schul_code,
                    "MLSV_YMD": req_date,  # 하루치만 조회
                }

                resp = requests.get(api_url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                if "mealServiceDietInfo" not in data:
                    # 주말/방학/시험기간 등: 급식 데이터 없음 (정상)
                    print(f"[INFO] 급식 정보 없음 — 학교:{schul_code}, 날짜:{req_date}")
                    continue

                meal_info = data["mealServiceDietInfo"]
                if len(meal_info) < 2:
                    print(f"[INFO] 급식 row 없음 — 학교:{schul_code}, 날짜:{req_date}")
                    continue

                rows = meal_info[1].get("row", [])
                if not rows:
                    print(f"[INFO] 급식 row 비어있음 — 학교:{schul_code}, 날짜:{req_date}")
                    continue

                all_rows.extend(rows)
                print(f"[학교={schul_code}] {len(rows)}건 급식 수집 완료 ({req_date})")

            except Exception as e:
                # 한 학교 실패 시
                # 일단은 에러 로그 찍고 계속 진행
                print(f"[WARN] 급식 API 오류 — 학교:{schul_code}, 날짜:{req_date}, 오류:{e}")
                continue

        print(f"[INFO] 날짜 {req_date} 기준 전체 급식 row 개수: {len(all_rows)}")
        return all_rows

    # --- 4. Snowflake 적재 (incremental INSERT) ---
    @task()
    def load_to_snowflake(rows: list):
        """
        - rows가 비어 있으면 아무 작업도 하지 않음
        - rows[0]의 키를 기준으로 테이블 스키마 생성 (모든 컬럼 VARCHAR)
        - RAW 계층 incremental
        """
        if not rows:
            print("적재할 급식 데이터 없음. Snowflake 적재 X")
            return

        try:
            # 1) 컬럼명은 첫 번째 row의 키에서 가져옴
            columns = list(rows[0].keys())

            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            conn = hook.get_conn()
            cursor = conn.cursor()

            full_table_name = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}"

            # 2) CREATE TABLE (모든 컬럼 VARCHAR)
            cols_ddl = ", ".join([f'"{col}" VARCHAR' for col in columns])
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {cols_ddl}
            )
            """
            print("CREATE TABLE IF NOT EXISTS 실행...")
            cursor.execute(create_sql)

            # 3) INSERT (RAW 중복 허용)
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
            raise RuntimeError(f"[Snowflake 적재 오류] MEAL_DIET_INFO_INCHEON 적재 실패: {e}")

    # --- 5. 태스크 의존성 설정 ---

    API_URL = BASE_URL
    API_KEY = Variable.get("NEIS_KEY")

    school_codes = get_school_codes()
    fetched_rows = fetch_meal_for_date(API_URL, API_KEY, school_codes)
    load_to_snowflake(fetched_rows)

