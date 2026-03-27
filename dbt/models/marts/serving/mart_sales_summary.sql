/*
  SERVING: mart_sales_summary
  ─────────────────────────────────────────────────────────
  Report requirement:
  | periode (YYYY-MM) | class (LOW/MEDIUM/HIGH) | model | total |

  Grain: 1 baris per (periode, class, model)
  Source: fact_sales

  Best practice:
  - Serving layer = pre-aggregated, BI-ready
  - Langsung dikonsumsi oleh dashboard
  - Materialized as TABLE (not view) untuk performance
*/

{{
    config(materialized='table')
}}

SELECT
    periode,
    price_class                     AS class,
    model,
    SUM(price)                      AS total,
    COUNT(*)                        AS total_transactions,
    CURRENT_TIMESTAMP()             AS dbt_loaded_at

FROM {{ ref('fact_sales') }}
GROUP BY periode, price_class, model
ORDER BY periode, price_class, model
