{{ config(
    alias='allergy',
    materialized='table'
) }}

WITH base AS (
    SELECT
        ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE,
        SCHUL_NM,
        MMEAL_SC_CODE,
        MMEAL_SC_NM,
        MLSV_YMD,
        DDISH_NM,

        -- DDISH_NM 전체에서 숫자 알레르기 번호 추출 (예: 1,2,5,6,10,13...)
        REGEXP_SUBSTR_ALL(DDISH_NM, '\\d+') AS raw_allergy_array
    FROM {{ source('stg_data', 'MEAL_DIET_INFO_ALL') }}
),

processed AS (
    SELECT
        *,
        -- 정렬 + 중복 제거
        ARRAY_SORT(ARRAY_DISTINCT(raw_allergy_array)) AS sorted_allergy_array
    FROM base
)

SELECT
    ATPT_OFCDC_SC_CODE,
    ATPT_OFCDC_SC_NM,
    SD_SCHUL_CODE,
    SCHUL_NM,
    MMEAL_SC_CODE,
    MMEAL_SC_NM,
    MLSV_YMD,
    DDISH_NM,

    -- 배열을 CSV 문자열로 변환 → "1,2,5,6,10,13,16,17"
    ARRAY_TO_STRING(sorted_allergy_array, ',') AS ALLERGY_LIST

FROM processed
