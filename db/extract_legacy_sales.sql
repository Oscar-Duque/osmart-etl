SELECT 
    v.VENTA as venta,
    v.F_EMISION AS fecha,
    v.USUHORA AS susuhora,
    v.Caja AS caja,
    v.USUARIO AS usuario,
    v.importe + v.IMPUESTO AS total,
    -- Real payment breakdown from flujo
    SUM(CASE WHEN f.concepto2 = 'TAR' AND f.ING_EG = 'I' THEN f.importe ELSE 0 END) AS tarjeta_in,
    SUM(CASE WHEN f.concepto2 = 'EFE' AND f.ING_EG = 'I' THEN f.importe ELSE 0 END) AS efectivo_in,
    SUM(CASE WHEN f.concepto2 NOT IN ('EFE', 'TAR') AND f.ING_EG = 'I' THEN f.importe ELSE 0 END) AS otros_in,
    COALESCE(c.importe, 0) AS cobranza_aplicada,
    SUM(CASE WHEN f.concepto2 <> 'TARJ' AND f.ING_EG = 'E' THEN f.importe ELSE 0 END) AS egresos
FROM ventas v
LEFT JOIN flujo f ON v.venta = f.venta
LEFT JOIN cobranza c ON v.venta = c.venta
WHERE v.ESTADO = 'CO' 
AND v.TIPO_DOC = 'REM'
AND v.CIERRE = 0
GROUP BY
    v.VENTA
ORDER BY v.VENTA;