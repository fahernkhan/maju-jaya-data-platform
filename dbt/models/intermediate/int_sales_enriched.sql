/*
  INTERMEDIATE: int_sales_enriched
  ─────────────────────────────────────────────────────────
  Tujuan: Enrich sales dengan customer info + price classification
  Source: stg_sales + stg_customers
  Business logic: price class categorization, dedup handling
*/

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

customers AS (
    SELECT
        customer_id,
        customer_name,
        customer_type
    FROM {{ ref('stg_customers') }}
),

-- Deduplicate: ambil record pertama per group
deduplicated AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.customer_id, s.model, s.invoice_date
            ORDER BY s.created_at ASC
        ) AS rn
    FROM sales s
)

SELECT
    d.vin,
    d.customer_id,
    c.customer_name,
    c.customer_type,
    d.model,
    d.invoice_date,
    d.price,

    -- Business logic: price classification (dari requirement soal)
    CASE
        WHEN d.price BETWEEN 100000000 AND 250000000 THEN 'LOW'
        WHEN d.price BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
        WHEN d.price > 400000000                     THEN 'HIGH'
        ELSE 'UNDEFINED'
    END AS price_class,

    -- Periode (YYYY-MM format dari requirement)
    DATE_FORMAT(d.invoice_date, '%Y-%m') AS periode,

    d.is_duplicate_suspect,
    d.created_at,

    -- Flag: is this the "canonical" record? (first per group)
    CASE WHEN d.rn = 1 THEN TRUE ELSE FALSE END AS is_canonical

FROM deduplicated d
LEFT JOIN customers c USING (customer_id)
WHERE d.invoice_date IS NOT NULL
