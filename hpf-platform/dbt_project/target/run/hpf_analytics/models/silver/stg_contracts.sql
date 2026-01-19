
  
    
    

    create  table
      "warehouse"."analytics"."stg_contracts__dbt_tmp"
  
    as (
      with source as ( select * from "warehouse"."analytics"."src_gr_dk_ht" )

select
    -- 1. 重命名ID列
    contract_no as contract_id,
    cust_id,

-- 2. 类型转换 (Decimal & Integer)
cast(loan_amt as decimal(18, 2)) as loan_amount,
cast(loan_period as integer) as loan_period_months,

-- 3. 日期处理
cast(loan_date as date) as loan_start_date,

-- 4. 状态映射 (核心业务逻辑)
case
    when status = '01' then 'active' -- 正常
    when status = '02' then 'overdue' -- 逾期
    when status = '03' then 'closed' -- 结清
    else 'unknown'
end as loan_status
from source
where
    loan_amt > 0 -- 5. 过滤掉金额异常的数据
    );
  
  