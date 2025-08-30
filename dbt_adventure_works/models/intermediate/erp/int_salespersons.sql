
with 
    persons as (
        select * 
        from {{ ref('stg_erp__person') }}
)

    , salesperson_filtered as (
        select *
        from persons
        where person_type in ('EM', 'SP')  -- Employee and Sales Person types
    )

    , transformed as (
        select
        -- Primary Key
            person_id as salesperson_id
        -- Salesperson Information
            , concat(first_name, ' ', last_name) as salesperson_name
            , person_type as employee_type
            , first_name
            , last_name
            , title
            , last_updated_at as source_last_updated_at
        
    from salesperson_filtered
)

select * 
from transformed
