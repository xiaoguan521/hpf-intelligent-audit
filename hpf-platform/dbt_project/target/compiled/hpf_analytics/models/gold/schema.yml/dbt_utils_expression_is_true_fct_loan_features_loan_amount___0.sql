



select
    1
from "warehouse"."analytics"."fct_loan_features"

where not(loan_amount >= 0)

