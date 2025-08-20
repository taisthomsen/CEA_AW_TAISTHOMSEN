
with 
    persons as (
      select * 
      from {{ ref('stg_erp__person') }}
    )

    , customers as (
        select *  
        from {{ ref('stg_erp__customer') }}
    )
    , stores as (
      select *
      from {{ ref('stg_erp__store') }}
    )
    , transformed as (
      select
        customers.customer_id
        , customers.person_id
        , customers.store_id
        , customers.territory_id
        , stores.sales_person_id
        , case 
            when customers.store_id is null
                then concat (
                  coalesce(persons.first_name, '')
                  , ' '
                  , coalesce(persons.middle_name, '')
                  , ' '
                  , coalesce(persons.last_name, '')
                )
            else stores.store_name
        end as customer_name
        , case
            when persons.customer_email_approval = 0
                then 'not approved'
            when persons.customer_email_approval = 1
                then 'approved for adventure works'
            else 'approved for adventure works and partners'
        end as customer_email_approval
        , case
            when customers.store_id is null
                then 'retail'
            else 'reseller'
        end as customer_type
        , customers.last_updated_at as source_last_updated_at
        from customers
        left join stores
          on customers.store_id = stores.store_id 
        left join persons
          on customers.person_id = persons.person_id
    )

select * 
from transformed
