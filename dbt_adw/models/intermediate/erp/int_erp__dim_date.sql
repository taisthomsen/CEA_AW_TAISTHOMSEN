{{
  config(
    materialized='view'
  )
}}

with date_spine as (
    select date_value
    from (
        select 
            date_trunc('day', dateadd(day, seq4(), '2011-01-01'::date)) as date_value
        from table(generator(rowcount => 1461))  -- 4 years of data
    )
    where date_value <= '2014-12-31'::date
),

final as (
    select
        -- Primary Key
        date_value as date_key,
        
        -- Date Information
        date_value as full_date,
        dayofweek(date_value) as day_of_week,
        dayname(date_value) as day_name,
        dayofmonth(date_value) as day_of_month,
        dayofyear(date_value) as day_of_year,
        weekofyear(date_value) as week_of_year,
        month(date_value) as month_number,
        monthname(date_value) as month_name,
        quarter(date_value) as quarter,
        year(date_value) as year,
        
        -- Business Logic
        case when dayofweek(date_value) in (1, 7) then true else false end as is_weekend,
        case when month(date_value) = 12 and dayofmonth(date_value) = 25 then true else false end as is_christmas,
        case when month(date_value) = 7 and dayofmonth(date_value) = 4 then true else false end as is_independence_day,
        
        -- Fiscal Year (assuming fiscal year starts in July)
        case when month(date_value) >= 7 then year(date_value) + 1 else year(date_value) end as fiscal_year,
        case when month(date_value) >= 7 then month(date_value) - 6 else month(date_value) + 6 end as fiscal_month
        
    from date_spine
)

select * from final
