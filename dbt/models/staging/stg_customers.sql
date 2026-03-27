/*
  STAGING: stg_customers
  Source: customers_raw
  Job: Standardize date format, flag suspect data, classify customer type
  Rule: 1:1 dengan source, tidak ada join
*/

WITH source AS (SELECT * FROM {{ source('raw_maju', 'customers') }})
SELECT
    CAST(id AS INT64) AS customer_id,
    name AS customer_name,
    CASE
        WHEN REGEXP_CONTAINS(dob, r'^\d{4}-\d{2}-\d{2}$') THEN SAFE.PARSE_DATE('%Y-%m-%d', dob)
        WHEN REGEXP_CONTAINS(dob, r'^\d{4}/\d{2}/\d{2}$') THEN SAFE.PARSE_DATE('%Y/%m/%d', dob)
        WHEN REGEXP_CONTAINS(dob, r'^\d{2}/\d{2}/\d{4}$') THEN SAFE.PARSE_DATE('%d/%m/%Y', dob)
        ELSE NULL
    END AS date_of_birth,
    CASE WHEN dob = '1900-01-01' OR dob IS NULL THEN TRUE ELSE FALSE END AS is_dob_suspect,
    CASE WHEN name LIKE 'PT %' OR name LIKE 'CV %' OR name LIKE 'UD %' OR dob IS NULL
         THEN 'corporate' ELSE 'individual' END AS customer_type,
    created_at
FROM source WHERE id IS NOT NULL