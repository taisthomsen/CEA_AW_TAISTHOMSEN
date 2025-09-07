
with 
    test_that as (
        select
            round(sum(unit_price * order_qty), 2) as sum_success
        from {{ ref('fct_sales') }}
        where order_date between '2011-01-01' and '2011-12-31'
        
    )

select *
from test_that
where sum_success != 12646112.16