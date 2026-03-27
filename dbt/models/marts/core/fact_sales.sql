/*
  FACT: fact_sales
  Grain: 1 baris per transaksi penjualan (VIN)
  FK: dim_customer(customer_id), dim_vehicle(model), dim_date(date_id)

  Best practice:
  - Fact table berisi measures (price) + FK ke dimensions
  - Filter: hanya canonical records (no duplicates)
*/

{{
    config(materialized='table')
}}

SELECT
    s.vin,
    s.customer_id,
    s.model,
    s.invoice_date          AS date_id,
    s.price,
    s.price_class,
    s.periode,
    s.customer_name,
    s.customer_type,
    s.is_duplicate_suspect,
    s.created_at,
    CURRENT_TIMESTAMP()     AS dbt_loaded_at

FROM {{ ref('int_sales_enriched') }} s
WHERE s.is_canonical = TRUE      -- Hanya record pertama per group (dedup)
  AND s.invoice_date IS NOT NULL
