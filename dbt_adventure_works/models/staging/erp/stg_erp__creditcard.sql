
with 
    source as (
        select * from {{ source('erp', 'payments') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(creditcardid as string) as credit_card_id
            -- Credit Card Information
            ,cast(cardtype as varchar) as credit_card_type
            ,cast(cardnumber as varchar) as credit_card_number
            -- System columns
            ,cast(modifieddate as date) as last_updated_at
            from source
        )

    select * 
    from renamed