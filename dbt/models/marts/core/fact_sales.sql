/*
  FACT: fact_sales
  Grain: 1 baris per VIN (deduplicated via is_canonical)
  Partitioned by invoice_date, clustered by customer_id + price_class
*/

{{ config(
    materialized='table',
    partition_by={"field": "invoice_date", "data_type": "date", "granularity": "month"},
    cluster_by=["customer_id", "price_class"]
) }}

SELECT
    vin,
    customer_id,
    model,
    invoice_date,
    price,
    price_class,
    periode,
    customer_name,
    customer_type,
    is_duplicate_suspect,
    created_at,
    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM {{ ref('int_sales_enriched') }}
WHERE is_canonical = TRUE
  AND invoice_date IS NOT NULL