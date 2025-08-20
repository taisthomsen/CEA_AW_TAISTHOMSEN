
with
    source as (
        select * 
        from {{ source('erp', 'salesorderheadersalesreason') }}
    )

    , transformed as (
        select
            --Foreign keys
            cast(salesorderid as string) as sales_order_id
            , cast(salesreasonid as string) as sales_reason_id
            --System columns
            , modifieddate as last_updated_at
            from source
    )

    select * 
    from transformed