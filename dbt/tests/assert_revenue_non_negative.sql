-- Custom test: revenue harus >= 0
-- dbt test: HARUS return 0 rows (jika return rows = FAIL)

SELECT vin, price
FROM {{ ref('fact_sales') }}
WHERE price < 0
