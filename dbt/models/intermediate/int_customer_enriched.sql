/*
  INTERMEDIATE: int_customer_enriched
  ─────────────────────────────────────────────────────────
  Tujuan: Join customer + address terbaru
  Source: staging models ONLY (tidak pernah langsung dari raw)
  Output: ephemeral (CTE, tidak persist ke DB)

  Kenapa ephemeral?
  - Intermediate adalah "workspace" — tidak dikonsumsi langsung
  - Hemat storage
  - Tapi logic JOIN tetap terpisah dari mart (mudah debug)

  Aturan intermediate layer:
  1. Source hanya dari ref() ke staging — bukan source()
  2. Boleh join multiple staging models
  3. Boleh ada business logic (kalkulasi, CASE WHEN)
  4. TIDAK dikonsumsi langsung oleh BI tool
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

    -- Denormalized address (dari latest address per customer)
    a.address,
    a.city,
    a.province,

    -- Full address string (untuk report)
    COALESCE(
        CONCAT_WS(', ', a.address, a.city, a.province),
        'Alamat tidak tersedia'
    ) AS full_address,

    c.created_at

FROM customers c
LEFT JOIN addresses a USING (customer_id)
