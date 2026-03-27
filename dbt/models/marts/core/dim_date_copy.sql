/*
  DIMENSION: dim_date
  Grain: 1 baris per tanggal
  Generated date spine: 2024-01-01 → 2027-12-31

  Best practice:
  - Date dimension SELALU di-generate, bukan derived dari data
  - Memastikan semua tanggal ada, bahkan yang tidak ada transaksi
*/

{{ config(materialized='table', schema='mart_maju') }}
SELECT date_id, EXTRACT(YEAR FROM date_id) AS year, EXTRACT(MONTH FROM date_id) AS month,
    EXTRACT(QUARTER FROM date_id) AS quarter, FORMAT_DATE('%Y-%m', date_id) AS year_month,
    FORMAT_DATE('%B', date_id) AS month_name, FORMAT_DATE('%A', date_id) AS day_name,
    EXTRACT(DAYOFWEEK FROM date_id) AS day_of_week,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_id) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2027-12-31')) AS date_id