
with 
    int_customers as (
        select * 
        from {{ ref('int_customers') }}
    )
    , generate_sk as (
      select
          {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk
          , customer_id
          , customer_name
          , customer_type
          , customer_email_approval
          , source_last_updated_at
          , current_timestamp() as updated_at
          , '{{ invocation_id }}' as dbt_run_id
      from int_customers
    )

    select * 
    from generate_sk
