{{ config(
    alias='allergy',
    materialized='table'
) }}

WITH raw AS (
    SELECT
        ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE,
        SCHUL_NM,
        MMEAL_SC_CODE,
        MMEAL_SC_NM,
        MLSV_YMD,
        DDISH_NM,

        -- (5.6), (9), (2.5.6.10.13) 등을 모두 추출하는 정규식
        REGEXP_SUBSTR_ALL(DDISH_NM, '\\(([0-9\\.]+)\\)') AS allergy_groups
    FROM {{ source('stg_data', 'MEAL_DIET_INFO_ALL') }}
),

split_groups AS (
    SELECT
        r.*,
        fg.value::string AS group_string
    FROM raw r,
    LATERAL FLATTEN(input => r.allergy_groups) fg
),

split_numbers AS (
    SELECT
        ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE,
        SCHUL_NM,
        MMEAL_SC_CODE,
        MMEAL_SC_NM,
        MLSV_YMD,
        DDISH_NM,
        SPLIT(REPLACE(REPLACE(group_string, '(', ''), ')', ''), '.') AS nums
    FROM split_groups
),

flattened AS (
    SELECT
        ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE,
        SCHUL_NM,
        MMEAL_SC_CODE,
        MMEAL_SC_NM,
        MLSV_YMD,
        DDISH_NM,
        TRY_TO_NUMBER(f.value::string) AS allergy_no
    FROM split_numbers,
    LATERAL FLATTEN(input => nums) f
    WHERE TRY_TO_NUMBER(f.value::string) BETWEEN 1 AND 18
)

SELECT DISTINCT *
FROM flattened
ORDER BY MLSV_YMD DESC, ALLERGY_NO
