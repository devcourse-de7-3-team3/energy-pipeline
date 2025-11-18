{{ config(
    alias='calorie', 
    materialized='table'
) }}

SELECT
    ATPT_OFCDC_SC_CODE,
    ATPT_OFCDC_SC_NM,
    -- 지역명 추출
    TRIM(REPLACE(ATPT_OFCDC_SC_NM, '광역시교육청', '')) AS REGION,
    SD_SCHUL_CODE,
    SCHUL_NM,
    -- 학교유형 구분
    CASE
        WHEN SCHUL_NM LIKE '%여자%' THEN '여고'
        ELSE '남/공학'
    END AS SCHUL_GENDER_TYPE,
    MMEAL_SC_CODE,
    MMEAL_SC_NM,
    MLSV_YMD,
    -- 일자기반 요일 값 생성
    -- 차트 라벨 정렬위해 'WEEOKDAY_ORDER'와 WEEOKDAY_ORDER 구분
    DAYOFWEEK(MLSV_YMD) AS WEEOKDAY_ORDER,
    CASE WEEOKDAY_ORDER
        WHEN 0 THEN '일'
        WHEN 1 THEN '월'
        WHEN 2 THEN '화'
        WHEN 3 THEN '수'
        WHEN 4 THEN '목'
        WHEN 5 THEN '금'
        WHEN 6 THEN '토'
        WHEN 7 THEN '일'
    END AS WEEOKDAY_KO,
    CAL_INFO,
    -- 차트에서 집계를 수행하기 위한 숫자데이터 정제
    TRY_TO_DECIMAL(TRIM(REPLACE(CAL_INFO, 'Kcal', '')), 10, 1) AS CAL_INFO_CLEANED
FROM {{ source('stg_data', 'MEAL_DIET_INFO_ALL') }}