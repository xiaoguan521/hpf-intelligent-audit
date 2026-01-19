
  
  create view "warehouse"."analytics"."src_repayments__dbt_tmp" as (
    select * from "warehouse"."oracle_data"."GR_DK_HKMX"
  );
