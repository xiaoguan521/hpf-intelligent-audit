{{ config( materialized='view' ) }}

-- 原始合同数据视图 (Bronze 层: ODS)
SELECT * FROM {{ source('oracle_data', 'GR_DK_HT') }}