
with 
    source as (
        select * from {{ source('erp', 'products') }}
    )

    , renamed as (
    select
        -- Primary Key
        cast(productid as string) as product_id
        -- Foreign Keys
        ,cast(productsubcategoryid as string) as product_subcategory_id
        -- Product Information          
        ,cast(name as varchar) as product_name
        ,finishedgoodsflag as finished_goods_flag
        ,cast(standardcost as float) as standard_cost
        ,cast(listprice as float) as unit_price
        ,cast(productline as varchar) as product_line
        ,cast(productmodelid as string) as product_model_id
        ,'size' as product_size
        ,cast(color as varchar) as product_color
        ,cast(sellstartdate as date) as sell_start_date
        ,cast(sellenddate as date) as sell_end_date
        -- System columns
        ,cast(rowguid as string) as rowguid
        ,cast(modifieddate as date) as last_updated_at
    from source
)

select * 
from renamed
