
with
    sales_reasons as (
        select * 
        from {{ ref('stg_erp__salesreason') }}
    )

    , sales_order_headers as (
        select * 
        from {{ ref('stg_erp__salesorderheadersalesreason') }}
    )
    , join_tables as (
        select
            sales_order_headers.sales_order_id
            , sales_order_headers.sales_reason_id
            , sales_reasons.sales_reason_name
            , sales_reasons.sales_reason_type
            , sales_reasons.last_updated_at as source_last_updated_at
        from sales_order_headers
        left join sales_reasons
        on sales_order_headers.sales_reason_id = sales_reasons.sales_reason_id
    )

    select * 
    from join_tables