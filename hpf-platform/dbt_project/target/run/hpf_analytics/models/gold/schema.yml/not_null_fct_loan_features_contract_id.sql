
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select contract_id
from "warehouse"."analytics"."fct_loan_features"
where contract_id is null



  
  
      
    ) dbt_internal_test