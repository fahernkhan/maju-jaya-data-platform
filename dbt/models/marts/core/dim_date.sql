/*
  DIMENSION: dim_date
  Grain: 1 baris per tanggal
  Generated date spine: 2024-01-01 → 2027-12-31

  Best practice:
  - Date dimension SELALU di-generate, bukan derived dari data
  - Memastikan semua tanggal ada, bahkan yang tidak ada transaksi
*/

{{
    config(materialized='table')
}}

WITH RECURSIVE date_spine AS (
    SELECT DATE('2024-01-01') AS date_id
    UNION ALL
    SELECT DATE_ADD(date_id, INTERVAL 1 DAY)
    FROM date_spine
    WHERE date_id < '2027-12-31'
)

SELECT
    date_id,
    YEAR(date_id)                        AS year,
    MONTH(date_id)                       AS month,
    QUARTER(date_id)                     AS quarter,
    DATE_FORMAT(date_id, '%Y-%m')        AS year_month,
    MONTHNAME(date_id)                   AS month_name,
    DAYNAME(date_id)                     AS day_name,
    DAYOFWEEK(date_id)                   AS day_of_week,
    CASE WHEN DAYOFWEEK(date_id) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine
