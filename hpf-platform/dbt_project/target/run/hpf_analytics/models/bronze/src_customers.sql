
  
  create view "warehouse"."analytics"."src_customers__dbt_tmp" as (
    select * from "warehouse"."oracle_data"."GR_JCXX"
  );
