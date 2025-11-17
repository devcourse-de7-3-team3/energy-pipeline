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
    CAL_INFO
FROM {{ source('stg_data', 'MEAL_DIET_INFO_ALL') }}