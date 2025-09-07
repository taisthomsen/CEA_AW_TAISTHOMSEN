
with
    int_sales as (
        select *
        from {{ ref('int_sales') }}
    )

    , dim_customers as (
        select customer_id
        from {{ ref('dim_customers') }}
    )

    , dim_products as (
        select product_id
        from {{ ref('dim_products') }}
    )
    , dim_locations as (
        select address_id
        from {{ ref('dim_locations') }}
    ) 
    , dim_salespersons as (
        select salesperson_id
        from {{ ref('dim_salespersons') }}
    )
    , dim_creditcards as (
        select credit_card_id
        from {{ ref('dim_creditcards') }}
    )

    , generate_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_order_detail_id']) }} as sales_sk
            , dim_customers.customer_id 
            , dim_products.product_id
            , dim_locations.address_id
            , dim_salespersons.salesperson_id
            , dim_creditcards.credit_card_id
            , sales_order_id
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
        left join dim_customers
        on int_sales.customer_id = dim_customers.customer_id
        left join dim_products
        on int_sales.product_id = dim_products.product_id
        left join dim_locations
        on int_sales.address_id = dim_locations.address_id
        left join dim_salespersons
        on int_sales.salesperson_id = dim_salespersons.salesperson_id
        left join dim_creditcards
        on int_sales.credit_card_id = dim_creditcards.credit_card_id
    )

select *
from generate_sk
