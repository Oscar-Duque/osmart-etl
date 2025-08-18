SELECT
  y.art_id,
  y.fecha,
  y.delta_cantidad,
  y.is_absolute,
  y.abs_stock_after
FROM
  (
    /* 1) Non-traspaso DELTA rows: keep as-is (exclude Ajuste; handled in #5) */
    SELECT
      r.art_id,
      r.fecha,
      r.delta_cantidad,
      0 AS is_absolute,
      NULL AS abs_stock_after
    FROM
      raw_stock_movements r
    WHERE
      r.tienda_id = :store_id
      AND r.tabla_origen <> 'Traspaso'
      AND r.tabla_origen <> 'ajusteinventario' -- ajustes handled below
      
    UNION ALL
      
      /* 2) Traspaso Entrada (non-cancel): keep all */
    SELECT
      r.art_id,
      r.fecha,
      r.delta_cantidad,
      0 AS is_absolute,
      NULL AS abs_stock_after
    FROM
      raw_stock_movements r
    WHERE
      r.tienda_id = :store_id
      AND r.tabla_origen = 'Traspaso'
      AND r.tipo_movimiento = 'Traspaso Entrada'
    
    UNION ALL
    
      /* 3) Traspaso Entrada Cancelado:
      keep ONLY if a prior Entrada exists on the SAME side,
      keep the EARLIEST cancel per (id_origen, art_id, tienda_id) */
    SELECT
      r.art_id,
      r.fecha,
      r.delta_cantidad,
      0 AS is_absolute,
      NULL AS abs_stock_after
    FROM
      raw_stock_movements r
      JOIN (
        SELECT
          tabla_origen,
          id_origen,
          art_id,
          tienda_id,
          MIN(fecha) AS min_fecha
        FROM
          raw_stock_movements
        WHERE
          tabla_origen = 'Traspaso'
          AND tipo_movimiento = 'Traspaso Entrada Cancelado'
          AND tienda_id = :store_id
        GROUP BY
          tabla_origen,
          id_origen,
          art_id,
          tienda_id
      ) m ON m.tabla_origen = r.tabla_origen
      AND m.id_origen = r.id_origen
      AND m.art_id = r.art_id
      AND m.tienda_id = r.tienda_id
      AND m.min_fecha = r.fecha
    WHERE
      r.tienda_id = :store_id
      AND r.tabla_origen = 'Traspaso'
      AND r.tipo_movimiento = 'Traspaso Entrada Cancelado'
      AND EXISTS (
        SELECT
          1
        FROM
          raw_stock_movements e
        WHERE
          e.tabla_origen = 'Traspaso'
          AND e.id_origen = r.id_origen
          AND e.art_id = r.art_id
          AND e.tienda_id = r.tienda_id
          AND e.tipo_movimiento = 'Traspaso Entrada'
          AND e.fecha <= r.fecha
      ) 
    
    UNION ALL
    
      /* 4) Traspaso Salida (non-cancel): keep all */
    SELECT
      r.art_id,
      r.fecha,
      r.delta_cantidad,
      0 AS is_absolute,
      NULL AS abs_stock_after
    FROM
      raw_stock_movements r
    WHERE
      r.tienda_id = :store_id
      AND r.tabla_origen = 'Traspaso'
      AND r.tipo_movimiento = 'Traspaso Salida' 
    
    UNION ALL
    
      /* 5) Traspaso Salida Cancelado:
      keep ONLY if a prior Salida exists on the SAME side,
      keep the EARLIEST cancel per (id_origen, art_id, tienda_id) */
    SELECT
      r.art_id,
      r.fecha,
      r.delta_cantidad,
      0 AS is_absolute,
      NULL AS abs_stock_after
    FROM
      raw_stock_movements r
      JOIN (
        SELECT
          tabla_origen,
          id_origen,
          art_id,
          tienda_id,
          MIN(fecha) AS min_fecha
        FROM
          raw_stock_movements
        WHERE
          tabla_origen = 'Traspaso'
          AND tipo_movimiento = 'Traspaso Salida Cancelado'
          AND tienda_id = :store_id
        GROUP BY
          tabla_origen,
          id_origen,
          art_id,
          tienda_id
      ) m ON m.tabla_origen = r.tabla_origen
      AND m.id_origen = r.id_origen
      AND m.art_id = r.art_id
      AND m.tienda_id = r.tienda_id
      AND m.min_fecha = r.fecha
    WHERE
      r.tienda_id = :store_id
      AND r.tabla_origen = 'Traspaso'
      AND r.tipo_movimiento = 'Traspaso Salida Cancelado'
      AND EXISTS (
        SELECT
          1
        FROM
          raw_stock_movements s0
        WHERE
          s0.tabla_origen = 'Traspaso'
          AND s0.id_origen = r.id_origen
          AND s0.art_id = r.art_id
          AND s0.tienda_id = r.tienda_id
          AND s0.tipo_movimiento = 'Traspaso Salida'
          AND s0.fecha <= r.fecha
      ) 
    
    UNION ALL
    
      /* 6) Ajuste de Inventario: ABSOLUTE event
      (requires you populated is_absolute=1 and abs_stock_after in raw) */
    SELECT
      r.art_id,
      r.fecha,
      NULL AS delta_cantidad,
      r.is_absolute,
      r.abs_stock_after
    FROM
      raw_stock_movements r
    WHERE
      r.tienda_id = :store_id
      AND r.tabla_origen = 'ajusteinventario'
      AND r.is_absolute = 1
  ) AS y
ORDER BY
  y.art_id,
  y.fecha;