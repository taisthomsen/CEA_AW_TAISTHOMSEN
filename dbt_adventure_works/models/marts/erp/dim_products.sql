with 
    int_products as (
        select * 
        from {{ ref('int_products') }}
    )

    , generate_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk
            , product_id
            , product_name
            , product_category_id
            , product_category_name
            , product_subcategory_id
            , product_subcategory_name
            , product_line
            , product_line_name
            , product_model_id
            , is_active
            , standard_cost
            , unit_price
            , product_size
            , finished_goods_flag
            , product_color
            , last_updated_at as source_last_updated_at
            , current_timestamp() as updated_at
            , '{{ invocation_id }}' as dbt_run_id
        from int_products
   )

select * 
from generate_sk
