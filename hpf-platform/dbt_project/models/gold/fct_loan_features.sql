{{ config( materialized='table' ) }}

-- 贷款特征宽表 (Gold 层: 用于 ML)
-- 注意: 实际项目中需要 join 更多表获取完整特征
SELECT
    contract_no,
    loan_amount,
    issue_date,
    loan_term_months,
    borrower_id,
    -- 特征工程示例
    CASE 
        WHEN loan_amount >= 1000000 THEN 'high'
        WHEN loan_amount >= 500000 THEN 'medium'
        ELSE 'low'
    END AS loan_amount_category,
    YEAR(issue_date) AS issue_year,
    MONTH(issue_date) AS issue_month,
    etl_updated_at
FROM {{ ref('stg_contracts') }}