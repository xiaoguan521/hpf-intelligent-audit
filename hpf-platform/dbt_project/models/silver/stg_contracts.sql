{{ config( materialized='table' ) }}

-- 清洗后的合同数据 (Silver 层: 标准化)
SELECT
    ID,
    CAST(DKHTBH AS VARCHAR) AS contract_no,
    CAST(JKRZJHM AS VARCHAR) AS borrower_id,
    CAST(DKJE AS DECIMAL(18,2)) AS loan_amount,
    CAST(FKRQ AS DATE) AS issue_date,
    CAST(DKYXQ AS INTEGER) AS loan_term_months,
    CURRENT_TIMESTAMP AS etl_updated_at
FROM {{ ref('src_contracts') }}
WHERE DKJE > 0  -- 数据质量过滤