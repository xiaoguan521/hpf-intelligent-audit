with contracts as (
    select * from "warehouse"."analytics"."stg_contracts"
),

customers as (
    select * from "warehouse"."analytics"."stg_customers"
),

repayments as (
    select * from "warehouse"."analytics"."stg_repayments"
),

-- ✨ 核心魔法：计算还款表现指标
repayment_metrics as (
    select
        contract_id,
        count(*) as total_periods,
        -- 计算逾期次数
        sum(
            case
                when repayment_status = 'overdue' then 1
                else 0
            end
        ) as overdue_count,
        -- 计算是否发生过逾期 (0/1)
        max(
            case
                when repayment_status = 'overdue' then 1
                else 0
            end
        ) as has_overdue_history
    from repayments
    group by
        contract_id
)

-- 🏁 最终组装：合同 + 客户 + 还款指标
select c.contract_id, c.loan_status, c.loan_amount, c.loan_period_months, c.loan_start_date, cust.age, cust.gender, cust.monthly_income,

-- New Enhanced Features
cust.occupation,
cust.city_tier,
cust.credit_score,
coalesce(rm.overdue_count, 0) as overdue_count,
coalesce(rm.total_periods, 0) as total_repayment_periods,

-- Categorical Features for Better Model
case
    when c.loan_amount / cust.monthly_income > 5 then 'high_risk'
    when c.loan_amount / cust.monthly_income > 3 then 'medium_risk'
    else 'low_risk'
end as dti_category,
case
    when cust.age < 25 then 'young'
    when cust.age between 25 and 40  then 'prime'
    when cust.age between 40 and 55  then 'mature'
    else 'senior'
end as age_group,
case
    when cust.monthly_income < 5000 then 'low_income'
    when cust.monthly_income < 10000 then 'middle_income'
    else 'high_income'
end as income_level,
case
    when c.loan_period_months > 240 then 'ultra_long'
    when c.loan_period_months > 120 then 'long_term'
    else 'short_term'
end as loan_duration_type,

-- Target Label
case
    when coalesce(rm.overdue_count, 0) > 0 then 1
    else 0
end as target_label, -- 这是 ML 的预测目标！
coalesce(rm.has_overdue_history, 0) as has_overdue_history_flag
from
    contracts c
    -- 关联客户表
    left join customers cust on c.cust_id = cust.cust_id
    -- 关联还款指标表
    left join repayment_metrics rm on c.contract_id = rm.contract_id