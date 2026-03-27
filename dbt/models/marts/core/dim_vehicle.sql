/*
  DIMENSION: dim_vehicle
  Grain: 1 baris per model kendaraan
  Source: int_sales_enriched (derived)
*/

{{
    config(materialized='table')
}}

SELECT DISTINCT
    model,
    -- Harga range per model
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price,
    -- Dominant price class
    CASE
        WHEN AVG(price) BETWEEN 100000000 AND 250000000 THEN 'LOW'
        WHEN AVG(price) BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
        WHEN AVG(price) > 400000000                     THEN 'HIGH'
        ELSE 'UNDEFINED'
    END AS default_price_class
FROM {{ ref('int_sales_enriched') }}
WHERE is_canonical = TRUE
GROUP BY model
