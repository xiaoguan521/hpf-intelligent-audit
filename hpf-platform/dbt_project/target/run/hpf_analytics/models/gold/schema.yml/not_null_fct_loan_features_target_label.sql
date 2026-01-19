
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select target_label
from "warehouse"."analytics"."fct_loan_features"
where target_label is null



  
  
      
    ) dbt_internal_test