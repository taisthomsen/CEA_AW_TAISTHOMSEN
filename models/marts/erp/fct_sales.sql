{{
  config(
    materialized='table'
  )
}}

with sales_order_header as (
    select * from {{ ref('stg_erp__salesorderheader') }}
),

sales_order_detail as (
    select * from {{ ref('stg_erp__salesorderdetail') }}
),

customer as (
    select * from {{ ref('dim_customer') }}
),

product as (
    select * from {{ ref('dim_product') }}
),

date as (
    select * from {{ ref('dim_date') }}
),

salesperson as (
    select * from {{ ref('dim_salesperson') }}
),

location as (
    select * from {{ ref('dim_location') }}
),

sales_reason as (
    select * from {{ ref('dim_sales_reason') }}
),

sales_joined as (
    select
        -- Primary Keys
        sod.sales_order_detail_id,
        soh.sales_order_id,
        
        -- Foreign Keys
        c.customer_key,
        p.product_key,
        d.date_key,
        sp.salesperson_key,
        l.location_key,
        sr.sales_reason_key,
        
        -- Order Information
        soh.order_date,
        soh.due_date,
        soh.ship_date,
        soh.status,
        soh.is_online_order,
        soh.purchase_order_number,
        soh.account_number,
        soh.credit_card_approval_code,
        
        -- Line Item Information
        sod.order_qty,
        sod.unit_price,
        sod.unit_price_discount,
        sod.discount,
        
        -- Calculated Metrics
        sod.order_qty as quantity_purchased,
        sod.unit_price * sod.order_qty as total_amount,
        sod.unit_price * sod.order_qty * (1 - coalesce(sod.discount, 0)) as net_amount,
        
        -- Header Level Metrics
        soh.subtotal,
        soh.tax_amount,
        soh.freight,
        soh.total_due,
        
        -- Metadata
        soh.comment,
        soh.rowguid,
        soh.modified_date,
        current_timestamp as dbt_updated_at,
        '{{ invocation_id }}' as dbt_run_id
        
    from sales_order_detail sod
    inner join sales_order_header soh on sod.sales_order_id = soh.sales_order_id
    inner join customer c on soh.customer_id = c.customer_key
    inner join product p on sod.product_id = p.product_key
    inner join date d on date_trunc('day', soh.order_date) = d.date_key
    left join salesperson sp on soh.salesperson_id = sp.salesperson_key
    left join location l on soh.billing_address_id = l.location_key
    left join sales_reason sr on sr.sales_reason_key = sr.sales_reason_key  -- This will need proper join logic
),

final as (
    select
        -- Primary Key
        sales_order_detail_id,
        
        -- Foreign Keys
        sales_order_id,
        customer_key,
        product_key,
        date_key,
        salesperson_key,
        location_key,
        sales_reason_key,
        
        -- Order Information
        order_date,
        due_date,
        ship_date,
        status,
        is_online_order,
        purchase_order_number,
        account_number,
        credit_card_approval_code,
        
        -- Line Item Information
        order_qty,
        unit_price,
        unit_price_discount,
        discount,
        
        -- Key Metrics (as per data dictionary)
        quantity_purchased,
        total_amount,
        net_amount,
        
        -- Header Level Metrics
        subtotal,
        tax_amount,
        freight,
        total_due,
        
        -- Metadata
        comment,
        rowguid,
        modified_date,
        dbt_updated_at,
        dbt_run_id
        
    from sales_joined
)

select * from final
