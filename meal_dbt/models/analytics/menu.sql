{{ config(
    alias='menu'
) }}


SELECT
    ATPT_OFCDC_SC_CODE,
    ATPT_OFCDC_SC_NM,
    SD_SCHUL_CODE,
    SCHUL_NM,
    MMEAL_SC_NM,
    MLSV_YMD,
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(DDISH_NM, '\\([^)]*\\)', ''),  -- 괄호 내용 제거
                        '<br/>', ''                                   -- <br/> 제거
                    ),
                    '-1', ''                                          -- '-1' 제거
                ),
                '[0-9@*+#\\.☆]', ''                                        -- *, +, ., ☆ 제거
            ),
            '\\s+', ','                                               -- 다중 공백 정리
        )
    ) AS DDISH_NM
FROM
    {{ source('STG_DATA', 'MEAL_DIET_INFO_ALL') }}

