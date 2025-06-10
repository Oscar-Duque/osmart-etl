from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert

def reset_ventas_limpias(engine):
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ventas_limpias"))
        conn.execute(text("""
            CREATE TABLE ventas_limpias (
                ven_id INT,
                tienda VARCHAR(100),
                fecha_hora DATETIME,
                caja VARCHAR(10),
                usuario VARCHAR(50),
                efectivo DECIMAL(20,2),
                tarjeta DECIMAL(20,2),
                otros DECIMAL(20,2),
                total_venta DECIMAL(20,2),
                source_db VARCHAR(100),
                source_system VARCHAR(50),
                extracted_at DATETIME,
                PRIMARY KEY (ven_id, tienda, source_system)
            );
        """))

def insert_on_conflict_update(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)
    stmt = stmt.on_duplicate_key_update(
        fecha_hora=stmt.inserted.fecha_hora,
        caja=stmt.inserted.caja,
        usuario=stmt.inserted.usuario,
        efectivo=stmt.inserted.efectivo,
        tarjeta=stmt.inserted.tarjeta,
        otros=stmt.inserted.otros,
        total_venta=stmt.inserted.total_venta,
        source_db=stmt.inserted.source_db,
        extracted_at=stmt.inserted.extracted_at
    )
    result = conn.execute(stmt)
    return result.rowcount

# def insert_chunk_with_upsert(df, engine, table_name):
#     if df.empty:
#         return  # Nothing to insert
    
#     # Build INSERT ... ON DUPLICATE KEY UPDATE query
#     insert_query = f"""
#     INSERT INTO {table_name} (
#         ven_id, tienda, fecha_hora, caja, usuario, efectivo, tarjeta, otros, total_venta, source, extracted_at
#     )
#     VALUES ({','.join(['%s'] * 10)})
#     ON DUPLICATE KEY UPDATE
#         fecha_hora = VALUES(fecha_hora),
#         caja = VALUES(caja),
#         usuario = VALUES(usuario),
#         efectivo = VALUES(efectivo),
#         tarjeta = VALUES(tarjeta),
#         otros = VALUES(otros),
#         total_venta = VALUES(total_venta),
#         source = VALUES(source),
#         extracted_at = VALUES(extracted_at);
#     """

#     # Convert DataFrame rows to list of tuples
#     rows = df[[
#         "ven_id", "tienda", "fecha_hora", "caja", "usuario", "efectivo", "tarjeta", "otros", "total_venta", "source", "extracted_at"
#     ]].to_records(index=False).tolist()

#     # Execute in a transaction
#     with engine.begin() as conn:
#         conn.execute(text(insert_query), rows)