with source as ( select * from "warehouse"."analytics"."src_gr_dk_hkmx" )

select
    contract_no as contract_id,
    cast(period as integer) as period_number,

-- 日期转换
cast(due_date as date) as due_date,
cast(actual_date as date) as actual_repayment_date,

-- 状态映射
case
    when status = '1' then 'normal'
    when status = '2' then 'overdue'
    else 'unknown'
end as repayment_status,

-- 衍生指标: 是否已还清 (实还日期非空)
case
    when actual_date is not null then true
    else false
end as is_paidoff
from source