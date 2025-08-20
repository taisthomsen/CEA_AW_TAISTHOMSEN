
with 
    source as (
        select * 
        from {{ source('erp', 'salesorderheaders') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(salesorderid as string) as sales_order_id        
        -- Foreign Keys
            ,cast(customerid as string) as customer_id
            ,cast(salespersonid as string) as salesperson_id
            ,cast (territoryid as string) as territory_id
            ,cast (creditcardid as string) as credit_card_id
            ,cast (shiptoaddressid as string) as shipping_address_id
        -- Order Information
            ,orderdate as order_date
            ,duedate as due_date
            ,shipdate as ship_date
            ,status as order_status
            ,cast(onlineorderflag as boolean) as is_online_order  
            ,cast(subtotal as numeric) as subtotal
            ,cast(taxamt as numeric) as tax_amount
            ,cast(freight as numeric) as freight
            ,cast(totaldue as numeric) as total_due
            -- System columns
            ,cast(rowguid as string) as rowguid
            ,cast(modifieddate as date) as last_updated_at

        from source
    )

    select * 
    from renamed
