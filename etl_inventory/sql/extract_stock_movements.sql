SELECT
  *
FROM
  (
    (
      SELECT
        dt.art_id AS art_id,
        h.fecha AS fecha,
        CASE
          WHEN t.sucOri = n.sucId
            AND h.movimiento = '0' THEN
            'Traspaso Salida'
          ELSE
            'Traspaso Salida Cancelado'
        END AS tipo_movimiento,
        0 AS is_absolute,
        CASE
          WHEN h.movimiento = '0' THEN
            dt.cantidad * - 1
          ELSE
            dt.cantidad
        END AS delta_cantidad,
        NULL AS abs_stock_after,
        h.id AS id_origen,
        h.tabla AS tabla_origen,
        u.nombre AS usuario
      FROM
        historial h
        JOIN traspaso t ON h.id = t.tra_id
        JOIN detallet dt ON dt.tra_id = t.tra_id
        JOIN nubecfg n ON t.sucOri = n.sucId
        JOIN usuario u ON u.usu_id = h.usu_id
      WHERE
        h.tabla = 'Traspaso'
        AND h.movimiento != '1'
        AND h.fecha >= :start_date
        AND h.fecha < DATE_ADD(:end_date, INTERVAL 1 DAY)
    ) UNION
    (
      SELECT
        dt.art_id,
        h.fecha,
        CASE
          WHEN t.sucOri != n.sucId
            AND h.movimiento = '1' THEN
            'Traspaso Entrada '
          ELSE
            'Traspaso Entrada Cancelado'
        END,
        0 AS is_absolute,
        CASE
          WHEN h.movimiento = '1' THEN
            dt.cantidad
          ELSE
            dt.cantidad * - 1
        END AS CANTIDAD,
        NULL AS abs_stock_after,
        h.id,
        h.tabla,
        u.nombre
      FROM
        historial h
        JOIN traspaso t ON h.id = t.tra_id
        JOIN detallet dt ON dt.tra_id = t.tra_id
        JOIN nubecfg n ON t.sucOri != n.sucId
        JOIN usuario u ON u.usu_id = h.usu_id
      WHERE
        h.tabla = 'Traspaso'
        AND h.movimiento != '0'
        AND h.fecha >= :start_date
        AND h.fecha < DATE_ADD(:end_date, INTERVAL 1 DAY)
    ) UNION
    (
      SELECT
        dn.art_id,
        h.fecha,
        CASE
          WHEN h.movimiento = '0' THEN
            'Nota de Crédito'
          ELSE
            'Nota de Crédito Cancelada'
        END,
        0 AS is_absolute,
        CASE
          WHEN h.movimiento = '0' THEN
            dn.cantidad
          ELSE
            dn.cantidad * - 1
        END AS CANTIDAD,
        NULL AS abs_stock_after,
        h.id,
        h.tabla,
        u.nombre
      FROM
        historial h
        JOIN detallen dn ON h.id = dn.ncr_id
        JOIN usuario u ON u.usu_id = h.usu_id
      WHERE
        h.tabla = 'NotaCredito'
        AND h.fecha >= :start_date
        AND h.fecha < DATE_ADD(:end_date, INTERVAL 1 DAY)
    ) UNION
    (
      SELECT
        a.art_id,
        h.fecha,
        'Ajuste de Inventario',
        1 AS is_absolute,
        NULL AS delta_cantidad,
        CAST(aj.exisActual AS SIGNED) AS abs_stock_after,
        h.id,
        h.tabla,
        u.nombre
      FROM
        historial h
        JOIN ajusteinventarioarticulo aj ON h.id = aj.ain_id
        JOIN articulo a ON aj.art_id = a.art_id
        JOIN usuario u ON u.usu_id = h.usu_id
      WHERE
        h.tabla = 'ajusteinventario'
        AND h.fecha >= :start_date
        AND h.fecha < DATE_ADD(:end_date, INTERVAL 1 DAY)
    ) UNION
    (
      SELECT
        dv.art_id,
        h.fecha,
        CASE
          WHEN h.movimiento = '0' THEN
            'Venta'
          ELSE
            'Venta Cancelada'
        END,
        0 AS is_absolute,
        CASE
          WHEN h.movimiento = '0' THEN
            dv.cantidad * - 1
          ELSE
            dv.cantidad
        END AS CANTIDAD,
        NULL AS abs_stock_after,
        h.id,
        h.tabla,
        u.nombre
      FROM
        historial h
        JOIN detallev dv ON h.id = dv.ven_id
        JOIN usuario u ON u.usu_id = h.usu_id
      WHERE
        h.tabla = 'Venta'
        AND h.fecha >= :start_date
        AND h.fecha < DATE_ADD(:end_date, INTERVAL 1 DAY)
    ) UNION
    (
      SELECT
        im.art_id,
        h.fecha,
        'Importar Articulo',
        0 AS is_absolute,
        (im.exisActual - im.exisAnterior) AS CANTIDAD,
        NULL AS abs_stock_after,
        h.id,
        h.tabla,
        u.nombre
      FROM
        historial h
        JOIN importararticulodetalle im ON h.id = im.ima_id
        JOIN usuario u ON u.usu_id = h.usu_id
      WHERE
        h.tabla = 'ImportarArticulo'
        AND h.fecha >= :start_date
        AND h.fecha < DATE_ADD(:end_date, INTERVAL 1 DAY)
    ) UNION
    (
      SELECT
        dc.art_id,
        h.fecha,
        CASE
          WHEN h.movimiento = '0' THEN
            'Compra'
          ELSE
            'Compra Cancelada'
        END,
        0 AS is_absolute,
        CASE
          WHEN h.movimiento = '0' THEN
            dc.cantidad
          ELSE
            dc.cantidad * - 1
        END AS CANTIDAD,
        NULL AS abs_stock_after,
        h.id,
        h.tabla,
        u.nombre
      FROM
        historial h
        JOIN detallec dc ON h.id = dc.com_id
        JOIN compra c ON c.com_id = dc.com_id
        JOIN usuario u ON u.usu_id = h.usu_id
      WHERE
        h.tabla = 'Compra'
        AND h.fecha >= :start_date
        AND h.fecha < DATE_ADD(:end_date, INTERVAL 1 DAY)
    ) UNION
    (
      SELECT
        dp.art_id,
        h.fecha,
        'Devolucion Proveedor',
        0 AS is_absolute,
        CASE
          WHEN h.movimiento = '0' THEN
            dp.cantidad * - 1
          ELSE
            dp.cantidad
        END AS CANTIDAD,
        NULL AS abs_stock_after,
        h.id,
        h.tabla,
        u.nombre
      FROM
        historial h
        JOIN notacreditopro ncp ON h.id = ncp.ncp_id
        JOIN detallenpro dp ON dp.ncp_id = ncp.ncp_id
        JOIN usuario u ON u.usu_id = h.usu_id
      WHERE
        h.tabla = 'NotaCreditoPro'
        AND h.fecha >= :start_date
        AND h.fecha < DATE_ADD(:end_date, INTERVAL 1 DAY)
    )
  ) AS movimientos
ORDER BY
  fecha;