/*
  FACT: fact_after_sales
  Grain: 1 baris per service ticket
  FK: dim_customer, dim_date, dim_service_type
  Filter: exclude orphan VIN dan tanggal masa depan
*/

{{ config(materialized='table', schema='mart_maju') }}
SELECT service_ticket, vin, customer_id, model, service_date, service_type AS service_type_code,
    is_vin_not_in_sales, is_future_date, created_at, CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM {{ ref('stg_after_sales') }}
