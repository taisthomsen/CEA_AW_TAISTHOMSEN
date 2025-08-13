{{
  config(
    materialized='table'
  )
}}

with location as (
    select * from {{ ref('int_erp__dim_location') }}
),

final as (
    select
        location_key,
        city,
        state_province,
        country_region,
        state_province_code,
        country_region_code,
        postal_code,
        address_line_1,
        address_line_2,
        spatial_location,
        rowguid,
        modified_date,
        
        -- Metadata
        current_timestamp as dbt_updated_at,
        '{{ invocation_id }}' as dbt_run_id
        
    from location
)

select * from final
