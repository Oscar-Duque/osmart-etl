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

