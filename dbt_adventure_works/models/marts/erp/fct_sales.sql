
with
    int_sales as (
        select *
        from {{ ref('int_sales') }}
    )

    , generate_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_order_detail_id']) }} as sales_sk
            , customer_id 
            , product_id
            , address_id
            , sales_order_id
            , salesperson_id
            , territory_id
            , payment_method
            , order_date
            , due_date
            , ship_date
            , order_status
            , is_online_order
            , subtotal
            , tax_amount
            , freight
            , total_due
            , order_qty
            , unit_price
            , unit_price_discount_percentage
            , unit_price_discount_value
            , source_last_updated_at
            , current_timestamp() as updated_at
        from int_sales
    )

select *
from generate_sk
