/*
  DIMENSION: dim_date
  Grain: 1 baris per tanggal (2024-01-01 → 2027-12-31)
  BigQuery: GENERATE_DATE_ARRAY bukan WITH RECURSIVE
*/

{{ config(materialized='table') }}

SELECT
    date_id,
    -- BigQuery: EXTRACT bukan YEAR()/MONTH()/QUARTER()
    EXTRACT(YEAR    FROM date_id) AS year,
    EXTRACT(MONTH   FROM date_id) AS month,
    EXTRACT(QUARTER FROM date_id) AS quarter,
    -- BigQuery: FORMAT_DATE bukan DATE_FORMAT
    FORMAT_DATE('%Y-%m', date_id) AS year_month,
    FORMAT_DATE('%B',    date_id) AS month_name,
    FORMAT_DATE('%A',    date_id) AS day_name,
    EXTRACT(DAYOFWEEK FROM date_id) AS day_of_week,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_id) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM UNNEST(
    GENERATE_DATE_ARRAY('2024-01-01', '2027-12-31', INTERVAL 1 DAY)
) AS date_id