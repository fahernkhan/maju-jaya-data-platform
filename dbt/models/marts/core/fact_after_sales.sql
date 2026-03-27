/*
  FACT: fact_after_sales
  Grain: 1 baris per service ticket
  FK: dim_customer, dim_date, dim_service_type
  Filter: exclude orphan VIN dan tanggal masa depan
*/

{{
    config(materialized='table')
}}

SELECT
    a.service_ticket,
    a.vin,
    a.customer_id,
    a.model,
    a.service_date          AS date_id,
    a.service_type          AS service_type_code,
    a.is_vin_not_in_sales,
    a.is_future_date,
    a.created_at,
    CURRENT_TIMESTAMP()     AS dbt_loaded_at

FROM {{ ref('stg_after_sales') }} a
-- Note: fact table BISA langsung dari staging jika tidak butuh enrichment
-- Dalam case ini, after_sales tidak perlu join complex di intermediate
