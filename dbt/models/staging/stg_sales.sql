/*
  STAGING: stg_sales
  Source: sales_raw
  Job: Cast price dari string ke integer, flag duplicates
*/

WITH source AS (SELECT * FROM {{ source('raw_maju', 'sales') }}),
cleaned AS (
    SELECT vin, CAST(customer_id AS INT64) AS customer_id, model,
        CAST(invoice_date AS DATE) AS invoice_date,
        SAFE_CAST(REPLACE(price, '.', '') AS INT64) AS price, created_at,
        COUNT(*) OVER (PARTITION BY customer_id, model, invoice_date) AS dup_count
    FROM source WHERE vin IS NOT NULL
)
SELECT vin, customer_id, model, invoice_date, price,
    CASE WHEN dup_count > 1 THEN TRUE ELSE FALSE END AS is_duplicate_suspect, created_at
FROM cleaned
