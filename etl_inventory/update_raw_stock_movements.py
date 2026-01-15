import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from pathlib import Path
from extract import extract_stock_movements
import logging

# Setup logging to file + console


SCRITP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parent.parent   # osmart-etl/
LOG_PATH = PROJECT_ROOT / "logs/update_raw_stock_movements.log"
CONFIG_PATH  = PROJECT_ROOT / "config_v2.json"
CONFIG = json.load(open(CONFIG_PATH))

get_last_raw_ts_sql = Path("sql/get_last_raw_ts.sql").read_text(encoding="utf-8")
set_last_raw_ts_sql = Path("sql/set_last_raw_ts.sql").read_text(encoding="utf-8")

log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler = logging.FileHandler(LOG_PATH)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

# Create connection to the cleaned data database (osmart_data)
analytics_source = CONFIG["cedis"]["analytics_source"]
analytics_engine = create_engine(
    f"mysql+pymysql://{analytics_source['user']}:"
    f"{analytics_source['password']}@{analytics_source['host']}:"
    f"{analytics_source['port']}/{analytics_source['database']}"
)

stores = ['vallarta', 'renacimiento', 'velazquez', 'coloso', 'zapata']

def extract_incremental_data(source, store_id, start_timestamp, ):
    """Extract data from start_timestamp to now"""
    end_date = datetime.now().date()
    start_date = start_timestamp.date() if start_timestamp else datetime(2024, 10, 26).date()
    
    # Create batch dates (can be daily for incremental updates)
    batch_dates = []
    current_date = start_date
    
    while current_date <= end_date:
        batch_dates.append((current_date.isoformat(), current_date.isoformat()))
        current_date += timedelta(days=1)
    
    return extract_stock_movements(source, store_id, batch_dates)

def main():
    logging.info("🔄 Starting incremental update...")
    
    for store in stores:
        store_id = CONFIG[store]["store_id"]
        source = CONFIG[store]["sicar_source"]
        logging.info(f"\n--- Processing store: {store} ---")
        
        # Get last processed timestamp
        with analytics_engine.begin() as conn:
            last_ts = conn.execute(
                text(get_last_raw_ts_sql),
                {'source_id': source["source_id"]}
            ).scalar()

        if last_ts:
            logging.info(f"📅 Last processed timestamp: {last_ts}")
            # Add a small buffer to avoid missing data due to timing issues
            start_ts = last_ts + timedelta(seconds=1)
        else:
            logging.warning("⚠️ No checkpoint found, starting from default date")
            start_ts = datetime(2024, 10, 26)
        
        logging.info(f"🚀 Extracting data from {start_ts} onwards...")
        
        # Extract and load new data
        total_rows = 0
        max_fecha = None
        
        try:
            for df in extract_incremental_data(source, store_id, start_ts):
                if not df.empty:
                    # Filter out records that are not newer than last_ts
                    if last_ts:
                        df = df[pd.to_datetime(df['fecha']) > last_ts]
                    
                    if not df.empty:
                        # Load to database
                        df.to_sql(
                            "raw_stock_movements", 
                            con=analytics_engine, 
                            if_exists="append", 
                            index=False,
                            method="multi"
                        )
                        
                        total_rows += len(df)
                        
                        # Track the maximum fecha for checkpoint update
                        batch_max = pd.to_datetime(df['fecha']).max()
                        if max_fecha is None or batch_max > max_fecha:
                            max_fecha = batch_max
            
            if total_rows > 0:
                logging.info(f"✅ Loaded {total_rows} new rows")
                
                # Update checkpoint with the maximum fecha processed
                if max_fecha:                    
                    with analytics_engine.begin() as conn:
                        conn.execute(
                            text(set_last_raw_ts_sql),
                            {"timestamp": max_fecha, 'source_name': source["source_name"]}
                        )
                        
                    logging.info(f"📌 Updated checkpoint to: {max_fecha}")
            else:
                logging.info(f"ℹ️ No new records found for {store}")
                
        except Exception as e:
            logging.error(f"❗️ Error processing {store}: {e}", exc_info=True)
            continue
    
    logging.info("🎉 Incremental update completed!")

if __name__ == "__main__":
    # Import pandas here since it's used in the main function
    import pandas as pd
    main()