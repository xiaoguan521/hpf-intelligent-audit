
  
  create view "warehouse"."analytics"."src_contracts__dbt_tmp" as (
    select * from "warehouse"."oracle_data"."GR_DK_HT"
  );
