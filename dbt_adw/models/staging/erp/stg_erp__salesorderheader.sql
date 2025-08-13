{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('erp', 'salesorderheader') }}
),

renamed as (
    select
        -- Primary Key
        salesorderid as sales_order_id,
        
        -- Foreign Keys
        customerid as customer_id,
        salespersonid as salesperson_id,
        billtoaddressid as billing_address_id,
        shiptoaddressid as shipping_address_id,
        
        -- Order Information
        orderdate as order_date,
        duedate as due_date,
        shipdate as ship_date,
        status,
        onlineorderflag as is_online_order,
        purchaseordernumber as purchase_order_number,
        accountnumber as account_number,
        creditcardapprovalcode as credit_card_approval_code,
        subtotal,
        taxamt as tax_amount,
        freight,
        totaldue as total_due,
        comment,
        rowguid,
        modifieddate as modified_date
        
    from source
)

select * from renamed
