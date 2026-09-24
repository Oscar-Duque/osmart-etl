INSERT INTO fact_ventas_diarias (
    fecha,
    tienda_id,
    total_ventas,
    numero_ventas,
    efectivo,
    tarjeta,
    otros
)
SELECT
    DATE(fecha_hora) AS fecha,
    tienda_id,
    SUM(COALESCE(total_venta, 0)) AS total_ventas,
    COUNT(*) AS numero_ventas,
    SUM(COALESCE(efectivo, 0)) AS efectivo,
    SUM(COALESCE(tarjeta, 0)) AS tarjeta,
    SUM(COALESCE(otros, 0)) AS otros
FROM ventas_limpias
WHERE fecha_hora IS NOT NULL
GROUP BY
    DATE(fecha_hora),
    tienda_id;