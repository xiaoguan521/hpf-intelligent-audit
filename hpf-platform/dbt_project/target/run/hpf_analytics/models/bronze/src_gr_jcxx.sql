
  
  create view "warehouse"."analytics"."src_gr_jcxx__dbt_tmp" as (
    select * from "warehouse"."main"."src_customers"
  );
