/*
  STAGING: stg_customer_addresses
  Source: customer_addresses_raw (dari pipeline Task 1)
  Job: Standardize city/province, get latest address per customer
*/

WITH source AS (
    SELECT * FROM {{ source('raw_maju', 'raw_customer_addresses') }}
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn
    FROM source
)
SELECT
    SAFE_CAST(id AS INT64) AS address_id,
    SAFE_CAST(customer_id AS INT64) AS customer_id,
    address,
    TRIM(city) AS city,
    TRIM(province) AS province,
    created_at
FROM ranked
WHERE rn = 1
