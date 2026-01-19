
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "warehouse"."analytics"."fct_loan_features"

where not(loan_amount >= 0)


  
  
      
    ) dbt_internal_test