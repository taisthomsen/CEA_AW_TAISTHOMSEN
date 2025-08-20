
with
    products as (
        select * 
        from {{ ref('stg_erp__product') }}
    )

    , product_categories as (
        select * 
        from {{ ref('stg_erp__productcategory') }}
    )

    , product_subcategories as (
        select * 
        from {{ ref('stg_erp__productsubcategory') }}
    )

    , join_1 as (
        select
            products.*
            , product_subcategories.product_category_id
            , product_subcategories.product_subcategory_name 
        from products
        left join product_subcategories
            on products.product_subcategory_id = product_subcategories.product_subcategory_id
)

    , final_join as (
        select 
            join_1.*
            , product_categories.product_category_name
            , case
                when product_line = 'R'
                    then 'Road'
                when product_line = 'M'
                    then 'Mountain'
                when product_line = 'T'
                    then 'Touring'
                when product_line = 'S'
                    then 'Standard'
                else 'not defined'
            end as product_line_name
            , case
                when sell_end_date is null
                    then 'active'
                else 'disabled'
            end as is_active
        from join_1
        left join product_categories
            on join_1.product_category_id = product_categories.product_category_id
    )

    select * 
    from final_join