
with
    source as (
        select * from {{ ref('stg_erp__creditcard') }}
    )

    , transformed as (
        select
            credit_card_id
            , credit_card_type
            , credit_card_number
            , last_updated_at
            ,current_timestamp() as updated_at
        from source
    )

    select * 
    from transformed