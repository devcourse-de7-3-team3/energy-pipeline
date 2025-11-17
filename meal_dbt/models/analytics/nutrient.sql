{{ config(
    schema='ANALYTICS',
    alias='NUTRIENT',
    materialized='table'
) }}

with base as (
    select
        ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE,
        SCHUL_NM,
        MMEAL_SC_CODE,
        MMEAL_SC_NM,
        MLSV_YMD,
        NTR_INFO
    from {{ ref('stg_meal_diet_info_all') }}
),

parsed as (
    select
        ATPT_OFCDC_SC_CODE      as atpt_ofcdc_sc_code,
        ATPT_OFCDC_SC_NM        as atpt_ofcdc_sc_nm,
        SD_SCHUL_CODE           as sd_schul_code,
        SCHUL_NM                as schul_nm,
        MMEAL_SC_CODE           as mmeal_sc_code,
        MMEAL_SC_NM             as mmeal_sc_nm,
        MLSV_YMD                as mlsv_ymd,

        -- 탄수화물(g)/130 * 100
        ( try_to_number(
            regexp_substr(NTR_INFO, '탄수화물\\(g\\) *: *([0-9.]+)', 1, 1, 'e', 1)
          ) / 130.0 ) * 100 AS carb_ratio_pct,

        -- 단백질(g)/60 * 100
        ( try_to_number(
            regexp_substr(NTR_INFO, '단백질\\(g\\) *: *([0-9.]+)', 1, 1, 'e', 1)
          ) / 60.0 ) * 100 AS protein_ratio_pct,

        -- 지방(g)/50 * 100
        ( try_to_number(
            regexp_substr(NTR_INFO, '지방\\(g\\) *: *([0-9.]+)', 1, 1, 'e', 1)
          ) / 50.0 ) * 100 AS fat_ratio_pct,

        -- 비타민A(R.E)/750 * 100
        ( try_to_number(
            regexp_substr(NTR_INFO, '비타민A\\(R.E\\) *: *([0-9.]+)', 1, 1, 'e', 1)
          ) / 750.0 ) * 100 AS vitamin_a_ratio_pct,

        -- 티아민(mg)/1.2 * 100
        ( try_to_number(
            regexp_substr(NTR_INFO, '티아민\\(mg\\) *: *([0-9.]+)', 1, 1, 'e', 1)
          ) / 1.2 ) * 100 AS thiamin_ratio_pct,

        -- 리보플라빈(mg)/1.5 * 100
        ( try_to_number(
            regexp_substr(NTR_INFO, '리보플라빈\\(mg\\) *: *([0-9.]+)', 1, 1, 'e', 1)
          ) / 1.5 ) * 100 AS riboflavin_ratio_pct,

        -- 비타민C(mg)/100 * 100
        ( try_to_number(
            regexp_substr(NTR_INFO, '비타민C\\(mg\\) *: *([0-9.]+)', 1, 1, 'e', 1)
          ) / 100.0 ) * 100 AS vitamin_c_ratio_pct,

        -- 칼슘(mg)/850 * 100
        ( try_to_number(
            regexp_substr(NTR_INFO, '칼슘\\(mg\\) *: *([0-9.]+)', 1, 1, 'e', 1)
          ) / 850.0 ) * 100 AS calcium_ratio_pct,

        -- 철분(mg)/14 * 100
        ( try_to_number(
            regexp_substr(NTR_INFO, '철분\\(mg\\) *: *([0-9.]+)', 1, 1, 'e', 1)
          ) / 14.0 ) * 100 AS iron_ratio_pct

    from base
)

select *
from parsed
