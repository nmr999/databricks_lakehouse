{{ config(
    materialized='incremental',
    unique_key='category',
    incremental_strategy='merge',
    comment='Incremental Gold layer aggregating KPIs from Silver orders data'
) }}

SELECT
    category,
    COUNT(id)      AS total_orders,
    SUM(amount)    AS total_amount,
    AVG(amount)    AS avg_amount,
    CURRENT_TIMESTAMP() AS gold_ingestion_date
FROM {{ ref('silver_orders') }}
GROUP BY category

{% if is_incremental() %}
  -- Process only new categories not yet present in Gold
  HAVING category NOT IN (SELECT category FROM {{ this }})
{% endif %}
