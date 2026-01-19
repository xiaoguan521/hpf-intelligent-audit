
  
  create view "warehouse"."analytics"."src_gr_dk_ht__dbt_tmp" as (
    select * from "warehouse"."main"."src_contracts"
  );
