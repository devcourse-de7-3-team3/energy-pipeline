{{ config(
    alias='MEAL_DIET_INFO_ALL'
) }}

with incheon as (
    select
        ATPT_OFCDC_SC_CODE::varchar           as ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM::varchar             as ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE::varchar                as SD_SCHUL_CODE,
        SCHUL_NM::varchar                     as SCHUL_NM,
        MMEAL_SC_CODE::varchar                as MMEAL_SC_CODE,
        MMEAL_SC_NM::varchar                  as MMEAL_SC_NM,

        try_to_date(MLSV_YMD, 'YYYYMMDD')     as MLSV_YMD,
        try_to_number(nullif(MLSV_FGR, ''))   as MLSV_FGR,

        DDISH_NM,
        ORPLC_INFO,
        CAL_INFO,
        NTR_INFO,

        try_to_date(MLSV_FROM_YMD, 'YYYYMMDD') as MLSV_FROM_YMD,
        try_to_date(MLSV_TO_YMD, 'YYYYMMDD')   as MLSV_TO_YMD,

        LOAD_DTM::varchar                     as LOAD_DTM
    from {{ source('raw_data', 'MEAL_DIET_INFO_INCHEON') }}
),

gwangju as (
    select
        ATPT_OFCDC_SC_CODE::varchar           as ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM::varchar             as ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE::varchar                as SD_SCHUL_CODE,
        SCHUL_NM::varchar                     as SCHUL_NM,
        to_varchar(MMEAL_SC_CODE)             as MMEAL_SC_CODE,   -- NUMBER → VARCHAR
        MMEAL_SC_NM::varchar                  as MMEAL_SC_NM,

        MLSV_YMD::date                        as MLSV_YMD,        -- 이미 DATE
        MLSV_FGR::number                      as MLSV_FGR,        -- 이미 NUMBER

        DDISH_NM,
        ORPLC_INFO,
        CAL_INFO::varchar                     as CAL_INFO,
        NTR_INFO,

        MLSV_FROM_YMD::date                   as MLSV_FROM_YMD,
        MLSV_TO_YMD::date                     as MLSV_TO_YMD,

        to_varchar(LOAD_DTM)                  as LOAD_DTM         -- DATE → VARCHAR
    from {{ source('raw_data', 'MEAL_DIET_INFO_GWANGJU') }}
),

daegu as (
    select
        ATPT_OFCDC_SC_CODE::varchar           as ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM::varchar             as ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE::varchar                as SD_SCHUL_CODE,
        SCHUL_NM::varchar                     as SCHUL_NM,
        MMEAL_SC_CODE::varchar                as MMEAL_SC_CODE,
        MMEAL_SC_NM::varchar                  as MMEAL_SC_NM,

        try_to_date(MLSV_YMD, 'YYYYMMDD')     as MLSV_YMD,
        try_to_number(nullif(MLSV_FGR, ''))   as MLSV_FGR,

        DDISH_NM,
        ORPLC_INFO,
        CAL_INFO,
        NTR_INFO,

        try_to_date(MLSV_FROM_YMD, 'YYYYMMDD') as MLSV_FROM_YMD,
        try_to_date(MLSV_TO_YMD, 'YYYYMMDD')   as MLSV_TO_YMD,

        LOAD_DTM::varchar                     as LOAD_DTM
    from {{ source('raw_data', 'MEAL_DIET_INFO_DAEGU') }}
),

busan as (
    select
        ATPT_OFCDC_SC_CODE::varchar           as ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM::varchar             as ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE::varchar                as SD_SCHUL_CODE,
        SCHUL_NM::varchar                     as SCHUL_NM,
        MMEAL_SC_CODE::varchar                as MMEAL_SC_CODE,
        MMEAL_SC_NM::varchar                  as MMEAL_SC_NM,

        try_to_date(MLSV_YMD, 'YYYYMMDD')     as MLSV_YMD,
        try_to_number(nullif(MLSV_FGR, ''))   as MLSV_FGR,

        DDISH_NM,
        ORPLC_INFO,
        CAL_INFO,
        NTR_INFO,

        try_to_date(MLSV_FROM_YMD, 'YYYYMMDD') as MLSV_FROM_YMD,
        try_to_date(MLSV_TO_YMD, 'YYYYMMDD')   as MLSV_TO_YMD,

        LOAD_DTM::varchar                     as LOAD_DTM
    from {{ source('raw_data', 'MEAL_DIET_INFO_BUSAN') }}
),

all_meals as (
    select * from incheon
    union all
    select * from gwangju
    union all
    select * from daegu
    union all
    select * from busan
)

select *
from all_meals
where MLSV_YMD >= date '2025-10-01'
