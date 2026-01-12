import jaydebeapi
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

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
            
def extract_sicar(config, store_name, batch_dates):
    try:
        # Use SQLAlchemy to connect to modern MySQL
        conn_str = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        engine = create_engine(conn_str)
        conn = engine.connect()
        
        # Load SICAR sales query from file
        with open("db/extract_sicar_sales.sql", "r") as f:
            query =  text(f.read())
        
        for start_date, end_date in batch_dates:
            try:
                print(f"🔄 Extracting SICAR sales for {store_name} from {start_date} to {end_date}...", end="", flush=True)
                df = pd.read_sql_query(
                    query,
                    conn,
                    params={"start_date": start_date, "end_date": end_date}
                )

                df["tienda"] = store_name
                df["source_db"] = config["database"]
                df["source_system"] = "sicar"
                df["extracted_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if not df.empty:
                    print(f" ✅ Extracted {len(df)} rows")
                    yield df
                else:
                    print(f" ⚠️ No data found in batch {start_date} to {end_date}")
            except Exception as e:
                print(f"❗️ Error extracting batch {start_date} to {end_date} for {store_name}: {e}")
    except Exception as conn_err:
        print(f"❗️ Database connection error for SICAR {store_name} at {config['host']}::{conn_err}")
    finally:
        if conn:
            conn.close()
            print("🔌 SICAR connection closed")