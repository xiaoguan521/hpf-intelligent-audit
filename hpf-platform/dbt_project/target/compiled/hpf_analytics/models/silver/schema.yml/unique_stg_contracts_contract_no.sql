
    
    

select
    contract_no as unique_field,
    count(*) as n_records

from "warehouse"."analytics"."stg_contracts"
where contract_no is not null
group by contract_no
having count(*) > 1


