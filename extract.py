import jaydebeapi
import pandas as pd

def extract_legacy(config):
    # source_data = []
    # conn = None

    try:
        conn = None
        conn = jaydebeapi.connect(
            "com.mysql.jdbc.Driver",
            f"jdbc:mysql://{config['host']}:{config['port']}/",
            [config["user"], config["password"]],
            config["driver"]
        )
        cursor = conn.cursor()

        for database in config["databases"]:
            try:
                print(f"🔄 Switching to database: {database}", end="", flush=True)
                cursor.execute(f"USE `{database}`")
                
                query = """
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
                """
                        
                cursor.execute(query)
                column_names = ["venta", "fecha", "usuhora", "caja", "usuario", "total", "tarjeta_in", "efectivo_in", "otros_in", "cobranza_aplicada", "egresos"]
                df = pd.DataFrame(cursor.fetchall(), columns = column_names)
                # df['source'] = f"{config['name']}:{database}"
                df["tienda"] = config['store']
                df['source_db'] = database
                df['source_system'] = "mybusiness"
                df['extracted_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if not df.empty:
                    # source_data.append(df)
                    print(f" ✅ Extracted {len(df)} rows from {database}")
                    yield df
                else:
                    print(f" ⚠️ No data found in {database}")

            except Exception as e:
                print(f"❗️ Error processing {database}: {e}")
                
    except Exception as conn_err:
        print(f"❗️ Database connection error for {config['name']} at {config['host']}:: {conn_err}")
    
    finally:
        if conn:
            conn.close()
            print("🔌 Connection closed")

    # if source_data:
    #     merged_df = pd.concat(source_data, ignore_index=True)
    #     return merged_df
    # else:
    #     print("⚠️ No data extracted from any database")
    #     return pd.DataFrame()  # Return an empty DataFrame if no data
    