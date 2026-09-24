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
    DATE(fecha_hora),
    tienda_id,
    SUM(COALESCE(total_venta, 0)),
    COUNT(*),
    SUM(COALESCE(efectivo, 0)),
    SUM(COALESCE(tarjeta, 0)),
    SUM(COALESCE(otros, 0))
FROM ventas_limpias
WHERE fecha_hora >= :fecha_inicio
GROUP BY
    DATE(fecha_hora),
    tienda_id;