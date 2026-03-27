/*
  DIMENSION: dim_service_type
  Grain: 1 baris per service type code
  Static/seed data — tidak berubah sering
*/

{{
    config(materialized='table')
}}

SELECT 'BP' AS service_type_code, 'Body Paint'            AS service_type_name, 'Perbaikan body dan cat kendaraan'        AS description
UNION ALL
SELECT 'PM', 'Periodic Maintenance', 'Servis berkala rutin sesuai jadwal'
UNION ALL
SELECT 'GR', 'General Repair',      'Perbaikan umum di luar jadwal berkala'
