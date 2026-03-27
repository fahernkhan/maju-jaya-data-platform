SELECT m.vin FROM {{ ref('mart_aftersales_priority') }} m
INNER JOIN {{ ref('fact_after_sales') }} f ON m.vin = f.vin
WHERE f.is_vin_not_in_sales = TRUE OR f.is_future_date = TRUE