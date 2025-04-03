import jaydebeapi
import pandas as pd

def extract_legacy(config):
    source_data = []
    
    try:
        conn = jaydebeapi.connect(
            "com.mysql.jdbc.Driver",
            f"jdbc:mysql://{config['host']}:{config['port']}/",
            [config["user"], config["password"]],
            config["driver"]
        )
        cursor = conn.cursor()

        for database in config["databases"]:
            try:
                print(f"🔄 Switching to database: {database}")
                cursor.execute(f"USE `{database}`")
                
                query = """
                    SELECT 
                    v.VENTA,
                    v.F_EMISION AS 'Fecha',
                    v.USUHORA,
                    v.Caja,
                    v.USUARIO,
                    v.importe + v.IMPUESTO AS 'Total',
                    -- Real payment breakdown from flujo
                    SUM(CASE WHEN f.concepto2 = 'TAR' AND f.ING_EG = 'I' THEN f.importe ELSE 0 END) AS tarjeta_in,
                    SUM(CASE WHEN f.concepto2 = 'EFE' AND f.ING_EG = 'I' THEN f.importe ELSE 0 END) AS efectivo_in,
                    SUM(CASE WHEN f.concepto2 NOT IN ('EFE', 'TAR') AND f.ING_EG = 'I' THEN f.importe ELSE 0 END) AS otros_in,
                    c.importe AS cobranza_aplicada,
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
                """
                        
                cursor.execute(query)
                # import pdb; pdb.set_trace()
                column_names = ["VENTA", "Fecha", "USUHORA", "Caja", "USUARIO", "Total", "Tarjeta_in", "Efectivo_in", "Otros_in", "Cobranza_aplicada", "Egresos"]
                df = pd.DataFrame(cursor.fetchall(), columns = column_names)
                df['source'] = f"{config['name']}:{database}"
                df['extracted_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if not df.empty:
                    source_data.append(df)
                    print(f"✅ Extracted {len(df)} rows from {database}")
                else:
                    print(f"⚠️ No data found in {database}")

            except Exception as e:
                print(f"❗️ Error processing {database}: {e}")
                
    except Exception as conn_err:
        print(f"❗️ Database connection error: {conn_err}")
    
    finally:
        if conn:
            conn.close()
            print("🔌 Connection closed")

    if source_data:
        merged_df = pd.concat(source_data, ignore_index=True)
        return merged_df
    else:
        print("⚠️ No data extracted from any database")
        return pd.DataFrame()  # Return an empty DataFrame if no data
    