import pandas as pd
from sqlalchemy import text, create_engine

def extract_stock_movements(source, batch_dates):
    conn = None
    
    try:
        conn_str = f"mysql+pymysql://{source['user']}:{source['password']}@{source['host']}:{source['port']}/{source['database']}"
        engine = create_engine(conn_str)
        conn = engine.connect()
    
        with open("sql/extract_stock_movements.sql", "r") as f:
            query = text(f.read())
    
        for start_date, end_date in batch_dates:
            try:
                print(f"🔄 Extracting stock movements for {source['store']} from {start_date} to {end_date}...", end="", flush=True)
                df = pd.read_sql_query(
                    query,
                    conn,
                    params={"start_date": start_date, "end_date": end_date}
                )
                
                df["tienda_id"] = source["store_id"]
                df["extracted_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if not df.empty:
                    print(f" ✅ Extracted {len(df)} rows")
                    yield df
                else:
                    print(f" ⚠️ No data found in batch {start_date} to {end_date}")
            except Exception as e:
                print(f"❗️ Error extracting batch {start_date} to {end_date} for {source['store']}: {e}")
    except Exception as conn_err:
        print(f"❗️ Database connection error for SICAR {source['store']} at {source['host']}::{conn_err}")
    finally:
        if conn:
            conn.close()
            print("🔌 SICAR connection closed")
