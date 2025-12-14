{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    comment='Incremental Silver orders from Bronze with null filtering and dedup by id'
) }}

with src as (

    select
        cast(id as int)            as id,
        cast(category as string)   as category,
        cast(amount as int)        as amount,
        cast(ingestion_date as timestamp) as ingestion_date
    from {{ ref('bronze_orders') }}
    where id is not null
      and category is not null
      and amount is not null

),

deduped as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by id
                order by ingestion_date desc
            ) as rn
        from src
    )
    where rn = 1

)

select
    id,
    category,
    amount,
    ingestion_date,
    current_timestamp() as silver_ingestion_date
from deduped
