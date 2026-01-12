import pandas as pd
import json
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import date, timedelta
from extract import extract_stock_movements

store = "zapata"
start_date = date(2025, 12, 1)
end_date = date.today() - timedelta(days=1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent   # osmart-etl/
CONFIG_PATH  = PROJECT_ROOT / "config_v2.json"
CONFIG = json.load(open(CONFIG_PATH))

extract_stock_movements_sql = Path("sql/extract_stock_movements.sql").read_text(encoding="utf-8")
get_max_raw_ts_sql = Path("sql/get_max_raw_ts.sql").read_text(encoding="utf-8")
set_last_raw_ts_sql = Path("sql/set_last_raw_ts.sql").read_text(encoding="utf-8")

analytics_source = CONFIG["cedis"]["analytics_source"]
analytics_engine = create_engine(
    f"mysql+pymysql://{analytics_source['user']}:"
    f"{analytics_source['password']}@{analytics_source['host']}:"
    f"{analytics_source['port']}/{analytics_source['database']}"
)

store_source = CONFIG[store]["sicar_source"]
store_engine = create_engine(
    f"mysql+pymysql://{store_source['user']}:"
    f"{store_source['password']}@{store_source['host']}:"
    f"{store_source['port']}/{store_source['database']}"
)

### Extraer datos

print(f"🚀 Extracting historical data for {store}")

try:
    conn = store_engine.connect()
    
    try:
        print(f"🔄 Extracting stock movements for {store} from {start_date} to {end_date}...", end="", flush=True)
        df = pd.read_sql_query(
            text(extract_stock_movements_sql),
            conn,
            params={"start_date": start_date, "end_date": end_date}
        )
        
        df["tienda_id"] = CONFIG[store]["store_id"]
        df["source_id"] = store_source["source_id"]
        df["extracted_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if not df.empty:
            print(f" ✅ Extracted {len(df)} rows")
        else:
            print(f" ⚠️ No data found in batch {start_date} to {end_date}")
    except Exception as e:
        print(f"❗️ Error extracting batch {start_date} to {end_date} for {store}: {e}")
except Exception as conn_err:
    print(f"❗️ Database connection error for SICAR {store} at {store_source['host']}::{conn_err}")
finally:
    if conn:
        conn.close()
        print("🔌 SICAR connection closed")

with analytics_engine.begin() as conn:
    ### TODO: Guardar los datos
    df.to_sql(
        "raw_stock_movements", 
        conn, 
        if_exists="append", 
        index=False,
        method="multi"
    )
    
    ### Guardar progreso de etl
    
    max_fecha = conn.execute(
        text(get_max_raw_ts_sql), 
        {'source_id': store_source["source_id"]}
    ).scalar()
    
    conn.execute(
        text(set_last_raw_ts_sql),
        {"ts": max_fecha, 'store_name': store}
    )