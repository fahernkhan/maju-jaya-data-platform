/*
  STAGING: stg_after_sales
  Source: raw_maju.raw_after_sales
  Job: Flag orphan VIN dan tanggal masa depan
*/

WITH source AS (
    SELECT * FROM {{ source('raw_maju', 'raw_after_sales') }}
),
sales_vins AS (
    SELECT DISTINCT vin FROM {{ source('raw_maju', 'raw_sales') }}
)

SELECT
    a.service_ticket,
    a.vin,
    a.customer_id,
    a.model,
    -- Raw data dari MySQL datetime → cast ke DATE langsung
    CAST(a.service_date AS DATE) AS service_date,
    a.service_type,
    CASE
        WHEN s.vin IS NULL THEN TRUE
        ELSE FALSE
    END AS is_vin_not_in_sales,
    CASE
        WHEN CAST(a.service_date AS DATE) > CURRENT_DATE()
        THEN TRUE
        ELSE FALSE
    END AS is_future_date,
    a.created_at

FROM source a
LEFT JOIN sales_vins s ON a.vin = s.vin
WHERE a.service_ticket IS NOT NULL