import json
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import date, timedelta
import calendar
from extract import extract_stock_movements
import pandas as pd

SCRITP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parent.parent   # osmart-etl/
LOG_PATH = PROJECT_ROOT / "logs/update_raw_stock_movements.log"
CONFIG_PATH  = PROJECT_ROOT / "config_v2.json"
CONFIG = json.load(open(CONFIG_PATH))

log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler = logging.FileHandler(LOG_PATH)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

get_max_raw_ts_sql = Path(SCRITP_DIR / "sql/get_max_raw_ts.sql").read_text(encoding="utf-8")
set_last_raw_ts_sql = Path(SCRITP_DIR / "sql/set_last_raw_ts.sql").read_text(encoding="utf-8")

# Create connection to the cleaned data database (osmart_data)
analytics_source = CONFIG["cedis"]["analytics_source"]
analytics_engine = create_engine(
    f"mysql+pymysql://{analytics_source['user']}:"
    f"{analytics_source['password']}@{analytics_source['host']}:"
    f"{analytics_source['port']}/{analytics_source['database']}",
    connect_args={"local_infile": 1}
)

# Delete and create raw_stock_movements table
raw_stock_movements_sql = Path(SCRITP_DIR / "sql/create_raw_stock_movements.sql").read_text(encoding="utf-8")
raw_stock_movements_queries = raw_stock_movements_sql.split(';')

with analytics_engine.begin() as conn:
    for query in raw_stock_movements_queries:
        if query.strip(): 
            conn.execute(text(query))

# Restart etl progress tracker for all stores
reset_last_raw_ts_sql = Path(SCRITP_DIR / "sql/reset_last_raw_ts.sql").read_text(encoding="utf-8")

with analytics_engine.begin() as conn:
    conn.execute(text(reset_last_raw_ts_sql))
    
stores = ['cedis', 'vallarta', 'vallarta_legacy', 'renacimiento', 'velazquez', 'coloso', 'zapata']

for store in stores:
    
    if store == 'vallarta_legacy':
        source = CONFIG['vallarta']["sicar_legacy_source"]
        store_id = CONFIG['vallarta']["store_id"]
    else:
        source = CONFIG[store]["sicar_source"]
        store_id = CONFIG[store]["store_id"]

    logging.info(f"\n--- Processing store: {store} ---")
    
    start_date = date(2024, 10, 26)
    end_date = date.today() - timedelta(days=1)
    batch_dates = []
    current_start = start_date
    
    while current_start <= end_date:
        # Get the last day of the month
        last_day = calendar.monthrange(current_start.year, current_start.month)[1]
        current_end = date(current_start.year, current_start.month, last_day)

        # Clip to end_date if needed
        if current_end > end_date:
            current_end = end_date

        batch_dates.append((current_start.isoformat(), current_end.isoformat()))

        # Move to the first of the next month
        next_month = current_start.month + 1
        next_year = current_start.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        current_start = date(next_year, next_month, 1)
    
    # Extract and load new data
    total_rows = 0
    max_fecha = None
    
    for df in extract_stock_movements(source, store_id, batch_dates):
        # 2. Load raw logs
        
        logging.info(f"Loading {len(df)} new rows...")

        df.to_sql(
            "raw_stock_movements", 
            con=analytics_engine, 
            if_exists="append", 
            index=False,
            method="multi",
            chunksize=5000
        )
        
        total_rows += len(df)
        
        # Track the maximum fecha for checkpoint update
        batch_max = pd.to_datetime(df['fecha']).max()
        if max_fecha is None or batch_max > max_fecha:
            max_fecha = batch_max
    
    if total_rows > 0:
        logging.info(f"✅ Loaded {total_rows} new rows")
        
        # Update checkpoint with the maximum fecha processed
        with analytics_engine.begin() as conn:        
            conn.execute(
                text(set_last_raw_ts_sql),
                {"timestamp": max_fecha, 'source_name': source["source_name"]}
            )
            
        logging.info(f"📌 Updated checkpoint to: {max_fecha}")
            
    else:
        logging.info(f"ℹ️ No records found for {store}")
    
    

logging.info("🎉 Seed completed!")