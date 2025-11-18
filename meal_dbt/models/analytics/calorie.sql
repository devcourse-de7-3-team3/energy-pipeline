{{ config(
    alias='calorie', 
    materialized='table'
) }}

SELECT
    ATPT_OFCDC_SC_CODE,
    ATPT_OFCDC_SC_NM,
    SD_SCHUL_CODE,
    SCHUL_NM,
    MMEAL_SC_CODE,
    MMEAL_SC_NM,
    MLSV_YMD,
    DAYOFWEEK(MLSV_YMD) AS WEEOKDAY_ORDER,
    CASE WEEOKDAY_ORDER
        WHEN 0 THEN '일'
        WHEN 1 THEN '월'
        WHEN 2 THEN '화'
        WHEN 3 THEN '수'
        WHEN 4 THEN '목'
        WHEN 5 THEN '금'
        WHEN 6 THEN '토'
        
    END AS WEEOKDAY_KO,
    CAL_INFO,
    TRY_TO_DECIMAL(TRIM(REPLACE(CAL_INFO, 'Kcal', '')), 10, 1) AS CAL_INFO_CLEANED
FROM {{ source('stg_data', 'MEAL_DIET_INFO_ALL') }}