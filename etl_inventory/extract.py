import pandas as pd
from pathlib import Path
from sqlalchemy import text, create_engine

def extract_stock_movements(source, store_id, batch_dates, script_dir):
    conn = None
    
    try:
        conn_str = f"mysql+pymysql://{source['user']}:{source['password']}@{source['host']}:{source['port']}/{source['database']}"
        engine = create_engine(conn_str)
        conn = engine.connect()
    
        query = Path(script_dir / "sql/extract_stock_movements.sql").read_text(encoding="utf-8")

        for start_date, end_date in batch_dates:
            try:
                print(f"🔄 Extracting stock movements for {source['source_name']} from {start_date} to {end_date}...", end="", flush=True)
                df = pd.read_sql_query(
                    text(query),
                    conn,
                    params={"start_date": start_date, "end_date": end_date, "source_id": source["source_id"], "tienda_id": store_id}
                )
 
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
