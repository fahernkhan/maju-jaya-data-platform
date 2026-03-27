/*
  DIMENSION: dim_customer
  Grain: 1 baris per customer
  Source: int_customer_enriched
*/

{{
    config(materialized='table')
}}

SELECT
    customer_id,
    customer_name,
    date_of_birth,
    is_dob_suspect,
    customer_type,
    city,
    province,
    full_address,
    created_at
FROM {{ ref('int_customer_enriched') }}
