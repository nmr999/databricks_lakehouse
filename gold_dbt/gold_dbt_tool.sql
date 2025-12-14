{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='category',
    on_schema_change='sync_all_columns',
    comment='Gold KPI aggregation by category from silver_orders'
) }}

with base as (

    select
        cast(category as string) as category,
        cast(id as string)       as id,
        cast(amount as double)   as amount
    from {{ ref('silver_orders') }}

),

agg as (

    select
        category,
        count(id)            as total_orders,
        sum(amount)          as total_amount,
        avg(amount)          as avg_amount,
        current_timestamp()  as gold_ingestion_date
    from base
    group by category

)

select * from agg
