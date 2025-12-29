SELECT 
    movimiento.ven_id,
    MAX(historial.fecha) AS fecha_hora,
    MAX(movimiento.caj_id) AS caja,
    MAX(usuario.nombre) AS usuario,
    SUM(CASE WHEN tipopago.tpa_id = 1 THEN movimiento.total ELSE 0 END) AS efectivo,
    SUM(CASE WHEN tipopago.tpa_id = 6 THEN movimiento.total ELSE 0 END) AS tarjeta,
    SUM(CASE WHEN tipopago.tpa_id NOT IN (1, 6) THEN movimiento.total ELSE 0 END) AS otros,
    SUM(movimiento.total) AS total_venta
FROM 
    movimiento
INNER JOIN historial ON movimiento.mov_id = historial.id
INNER JOIN tipopago ON movimiento.tpa_id = tipopago.tpa_id
INNER JOIN usuario ON historial.usu_id = usuario.usu_id
WHERE 
    historial.tabla = 'Movimiento'
    AND movimiento.tipo = 1
    AND movimiento.status = 1
    AND movimiento.ven_id IS NOT NULL
    AND historial.fecha  >= :start_date
    AND historial.fecha  < DATE_ADD(:end_date, INTERVAL 1 DAY)
GROUP BY 
    movimiento.ven_id
ORDER BY 
    fecha_hora;