
with 
    int_salespersons as (
        select * 
        from {{ ref('int_salespersons') }}
    )

    , transformed as (
        select
            salesperson_id
            , salesperson_name
            , employee_type
            , first_name
            , last_name
            , title
            , source_last_updated_at
            , current_timestamp() as updated_at
        from int_salespersons
    )

select * 
from transformed
