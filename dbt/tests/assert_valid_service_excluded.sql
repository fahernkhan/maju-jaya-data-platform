-- Custom test: orphan VIN dan future date TIDAK boleh ada di serving layer
-- Harus return 0 rows

SELECT *
FROM {{ ref('mart_aftersales_priority') }} m
WHERE EXISTS (
    SELECT 1 FROM {{ ref('fact_after_sales') }} f
    WHERE f.vin = m.vin
      AND (f.is_vin_not_in_sales = TRUE OR f.is_future_date = TRUE)
)
