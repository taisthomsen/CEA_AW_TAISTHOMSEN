
with 
    int_sales_reasons as (
        select * 
        from {{ ref('int_salesreasons') }}
    )

    , generate_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_order_id','sales_reason_id']) }} as sales_reason_sk
            , sales_order_id
            , sales_reason_id
            , sales_reason_name
            , sales_reason_type
            , source_last_updated_at
            , current_timestamp() as updated_at
        from int_sales_reasons
    )

    select * 
    from generate_sk
