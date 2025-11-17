{{ config(
    alias='MEAL_DIET_INFO_TEST',
) }}

with incheon as (
    select
        ATPT_OFCDC_SC_CODE,
        ATPT_OFCDC_SC_NM,
        SD_SCHUL_CODE,
        SCHUL_NM,
        MMEAL_SC_CODE,
        MMEAL_SC_NM,

        -- 날짜/숫자 컬럼은 안전하게 TRY_ 함수로 캐스팅
        try_to_date(MLSV_YMD, 'YYYYMMDD')            as MLSV_YMD,
        try_to_number(nullif(MLSV_FGR, ''))          as MLSV_FGR,

        DDISH_NM,
        ORPLC_INFO,
        CAL_INFO,
        NTR_INFO,

        try_to_date(MLSV_FROM_YMD, 'YYYYMMDD')       as MLSV_FROM_YMD,
        try_to_date(MLSV_TO_YMD, 'YYYYMMDD')         as MLSV_TO_YMD,

        -- LOAD_DTM은 일단 VARCHAR 그대로 두고 나중에 형식 보고 TIMESTAMP로 바꿔도 됨
        LOAD_DTM
    from {{ source('raw_data', 'MEAL_DIET_INFO_INCHEON') }}
)

select *
from incheon