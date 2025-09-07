
with 
    int_customers as (
        select * 
        from {{ ref('int_customers') }}
    )
    , transformed as (
      select
           customer_id
          , customer_name
          , customer_type
          , customer_email_approval
          , source_last_updated_at
          , current_timestamp() as updated_at
      from int_customers
    )

    select * 
    from transformed
