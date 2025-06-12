import jaydebeapi
import pandas as pd

def extract_legacy(config):
    try:
        conn = None
        conn = jaydebeapi.connect(
            "com.mysql.jdbc.Driver",
            f"jdbc:mysql://{config['host']}:{config['port']}/",
            [config["user"], config["password"]],
            config["driver"]
        )
        cursor = conn.cursor()
        
        # Load legacy sales query from file
        with open("db/extract_legacy_sales.sql", "r") as f:
            query = f.read()

        for database in config["databases"]:
            try:
                print(f"🔄 Switching to database: {database}", end="", flush=True)
                cursor.execute(f"USE `{database}`")
                      
                cursor.execute(query)
                column_names = ["venta", "fecha", "usuhora", "caja", "usuario", "total", "tarjeta_in", "efectivo_in", "otros_in", "cobranza_aplicada", "egresos"]
                df = pd.DataFrame(cursor.fetchall(), columns = column_names)
                df["tienda"] = config['store']
                df['source_db'] = database
                df['source_system'] = "mybusiness"
                df['extracted_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if not df.empty:
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
