/*
  STAGING: stg_after_sales
  Source: after_sales_raw
  Job: Flag orphan VIN dan tanggal masa depan
*/

WITH source AS (
    SELECT * FROM {{ source('raw_maju', 'after_sales') }}
),
sales_vins AS (
    SELECT DISTINCT vin FROM {{ source('raw_maju', 'sales') }}
)

SELECT
    a.service_ticket,
    a.vin,
    a.customer_id,
    a.model,

    -- FIX DISINI
    PARSE_DATE('%Y%m%d', CAST(a.service_date AS STRING)) AS service_date,

    a.service_type,

    CASE 
        WHEN s.vin IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_vin_not_in_sales,

    CASE 
        WHEN PARSE_DATE('%Y%m%d', CAST(a.service_date AS STRING)) > CURRENT_DATE()
        THEN TRUE 
        ELSE FALSE 
    END AS is_future_date,

    a.created_at

FROM source a
LEFT JOIN sales_vins s ON a.vin = s.vin
WHERE a.service_ticket IS NOT NULL
