SELECT MAX(fecha) AS max_fecha
FROM raw_stock_movements
WHERE source_id = :source_id;