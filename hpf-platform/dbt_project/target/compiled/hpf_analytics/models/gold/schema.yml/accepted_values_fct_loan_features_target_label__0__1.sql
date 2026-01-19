
    
    

with all_values as (

    select
        target_label as value_field,
        count(*) as n_records

    from "warehouse"."analytics"."fct_loan_features"
    group by target_label

)

select *
from all_values
where value_field not in (
    '0','1'
)


