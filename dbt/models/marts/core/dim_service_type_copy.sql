/*
  DIMENSION: dim_service_type
  Grain: 1 baris per service type code
  Static/seed data — tidak berubah sering
*/

{{ config(materialized='table', schema='mart_maju') }}
SELECT 'BP' AS service_type_code, 'Body Paint' AS service_type_name, 'Perbaikan body/cat' AS description
UNION ALL SELECT 'PM', 'Periodic Maintenance', 'Servis berkala rutin'
UNION ALL SELECT 'GR', 'General Repair', 'Perbaikan umum'
