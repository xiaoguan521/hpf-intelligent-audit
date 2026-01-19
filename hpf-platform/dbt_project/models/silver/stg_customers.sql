with source as ( select * from {{ ref('src_gr_jcxx') }} )

select
    cust_id,
    name as customer_name,
    cast(age as integer) as age,
    cast(income as decimal(18, 2)) as monthly_income,

-- New Enhanced Features
occupation, city_tier, credit_score,

-- 性别映射
case
    when gender = '1' then 'M'
    when gender = '2' then 'F'
    else 'U'
end as gender
from source