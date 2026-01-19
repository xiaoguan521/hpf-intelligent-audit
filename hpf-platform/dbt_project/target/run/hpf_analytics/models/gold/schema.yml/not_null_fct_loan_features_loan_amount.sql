
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select loan_amount
from "warehouse"."analytics"."fct_loan_features"
where loan_amount is null



  
  
      
    ) dbt_internal_test