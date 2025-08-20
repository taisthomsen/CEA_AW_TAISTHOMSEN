
with 
    int_salespersons as (
        select * 
        from {{ ref('int_salespersons') }}
    )

    , generate_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['salesperson_pk']) }} as salesperson_sk
            , salesperson_pk
            , salesperson_name
            , employee_type
            , first_name
            , last_name
            , title
            , source_last_updated_at
            , current_timestamp() as updated_at
            , '{{ invocation_id }}' as dbt_run_id
        from int_salespersons
    )

select * 
from generate_sk
