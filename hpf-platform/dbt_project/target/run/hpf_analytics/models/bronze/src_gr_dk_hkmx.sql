
  
  create view "warehouse"."analytics"."src_gr_dk_hkmx__dbt_tmp" as (
    select * from "warehouse"."main"."src_repayments"
  );
