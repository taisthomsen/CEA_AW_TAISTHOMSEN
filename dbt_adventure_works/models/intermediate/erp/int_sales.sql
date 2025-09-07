
with
    sales_order_headers as (
        select * 
        from {{ ref('stg_erp__salesorderheader') }}
    )

    , sales_order_details as (
        select * 
        from {{ ref('stg_erp__salesorderdetail') }}
    )

    , transformed as (
        select
            sales_order_headers.sales_order_id
            , sales_order_headers.customer_id
            , sales_order_headers.salesperson_id
            , sales_order_headers.territory_id
            , case
                when sales_order_headers.credit_card_id is not null
                    then 'credit card'
                else 'other payment method'
            end as payment_method
            , sales_order_headers.credit_card_id
            , sales_order_headers.address_id   
            , cast(sales_order_headers.order_date as date) as order_date
            , cast(sales_order_headers.due_date as date) as due_date
            , cast(sales_order_headers.ship_date as date) as ship_date
            , case
                when sales_order_headers.order_status = 1 
                    then 'In Process'
                when sales_order_headers.order_status = 2  
                    then 'Approved'
                when sales_order_headers.order_status = 3 
                    then 'Backordered'
                when sales_order_headers.order_status = 4 
                    then 'Rejected'
                when sales_order_headers.order_status = 5 
                    then 'Shipped'
                when sales_order_headers.order_status = 6 
                    then 'Cancelled'
                else 'Unknown'
            end as order_status
            , sales_order_headers.is_online_order
            , cast(sales_order_headers.subtotal as float) as subtotal
            , cast(sales_order_headers.tax_amount as float) as tax_amount
            , cast(sales_order_headers.freight as float) as freight
            , cast(sales_order_headers.total_due as float) as total_due     
            , sales_order_details.sales_order_detail_id
            , sales_order_details.product_id
            , sales_order_details.special_offer_id
            , cast(sales_order_details.order_qty as integer) as order_qty
            , cast(sales_order_details.unit_price as float) as unit_price
            , case
                when sales_order_details.unit_price_discount != 0
                    then cast(sales_order_details.unit_price_discount as float)
                else null
            end as unit_price_discount_percentage
            , round(sales_order_details.unit_price_discount * sales_order_details.unit_price * sales_order_details.order_qty, 3) as unit_price_discount_value
            , sales_order_details.last_updated_at as source_last_updated_at
        from sales_order_details
        left join sales_order_headers
            on sales_order_details.sales_order_id = sales_order_headers.sales_order_id  
)

select * 
from transformed