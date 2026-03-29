/*
  INTERMEDIATE: int_customer_enriched
  Tujuan: Join customer + address terbaru
  Materialized: ephemeral (CTE only, tidak persist ke DB)
*/

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

addresses AS (
    SELECT * FROM {{ ref('stg_customer_addresses') }}
)

SELECT
    c.customer_id,
    c.customer_name,
    c.date_of_birth,
    c.is_dob_suspect,
    c.customer_type,
    a.address,
    a.city,
    a.province,
    -- BigQuery: CONCAT_WS tidak ada, pakai CONCAT dengan null handling
    COALESCE(
        CONCAT(a.address, ', ', a.city, ', ', a.province),
        'Alamat tidak tersedia'
    ) AS full_address,
    c.created_at

FROM customers c
LEFT JOIN addresses a USING (customer_id)