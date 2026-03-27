/*
  SERVING: mart_aftersales_priority
  ─────────────────────────────────────────────────────────
  Report requirement:
  | periode (YYYY) | vin | customer_name | address | count_service | priority |

  Grain: 1 baris per (year, vin)
  Filter: exclude orphan VIN + tanggal masa depan
  Source: fact_after_sales + dim_customer
*/

{{
    config(materialized='table')
}}

WITH valid_services AS (
    -- Filter: hanya servis yang valid
    SELECT *
    FROM {{ ref('fact_after_sales') }}
    WHERE is_vin_not_in_sales = FALSE    -- VIN harus ada di sales
      AND is_future_date = FALSE          -- Bukan tanggal masa depan
),

aggregated AS (
    SELECT
        YEAR(a.date_id)         AS periode,
        a.vin,
        a.customer_id,
        COUNT(a.service_ticket) AS count_service
    FROM valid_services a
    GROUP BY YEAR(a.date_id), a.vin, a.customer_id
)

SELECT
    agg.periode,
    agg.vin,
    c.customer_name,
    c.full_address                  AS address,
    agg.count_service,
    CASE
        WHEN agg.count_service > 10 THEN 'HIGH'
        WHEN agg.count_service >= 5  THEN 'MED'
        ELSE 'LOW'
    END                             AS priority,
    CURRENT_TIMESTAMP()             AS dbt_loaded_at

FROM aggregated agg
LEFT JOIN {{ ref('dim_customer') }} c USING (customer_id)
ORDER BY agg.periode, agg.count_service DESC
