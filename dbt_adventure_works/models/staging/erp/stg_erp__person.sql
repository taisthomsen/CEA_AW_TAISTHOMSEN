
with 
    source as (
        select * 
        from {{ source('erp', 'persons') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(businessentityid as string) as person_id
            --Person Information
            ,cast(persontype as varchar)  as person_type
            ,namestyle as name_style
            ,cast(firstname as varchar) as first_name
            ,cast(middlename as varchar) as middle_name
            ,cast(lastname as varchar) as last_name
            ,cast(title as varchar) as title
            ,cast(emailpromotion as integer) as customer_email_approval
            -- System columns
            ,cast(rowguid as string) as rowguid
            ,cast(modifieddate as date) as last_updated_at  
            from source
        )

    select * 
    from renamed
