import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from pathlib import Path
from extract import extract_stock_movements

SCRITP_DIR = Path(__file__).resolve().parent

PROJECT_ROOT = Path(__file__).resolve().parent.parent   # osmart-etl/
CONFIG_PATH  = PROJECT_ROOT / "config.json"
CONFIG = json.load(open(CONFIG_PATH))

# Create connection to the cleaned data database (osmart_data)
db_config = CONFIG["analytics_db"]
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

def get_last_processed_timestamp(store_name):
    """Get the last processed timestamp for a store from the checkpoint table"""
    get_last_ts_sql = Path(SCRITP_DIR / "sql/get_last_raw_ts.sql").read_text(encoding="utf-8")
    
    with engine.begin() as conn:
        result = conn.execute(
            text(get_last_ts_sql),
            {'store_name': store_name}
        ).scalar()
        
        return result

def update_last_processed_timestamp(store_name, timestamp):
    """Update the last processed timestamp for a store"""
    set_last_raw_ts_sql = Path(SCRITP_DIR / "sql/set_last_raw_ts.sql").read_text(encoding="utf-8")
    
    with engine.begin() as conn:
        conn.execute(
            text(set_last_raw_ts_sql),
            {"ts": timestamp, 'store_name': store_name}
        )

def extract_incremental_data(source, start_timestamp):
    """Extract data from start_timestamp to now"""
    end_date = datetime.now().date()
    start_date = start_timestamp.date() if start_timestamp else datetime(2024, 10, 26).date()
    
    # Create batch dates (can be daily for incremental updates)
    batch_dates = []
    current_date = start_date
    
    while current_date <= end_date:
        batch_dates.append((current_date.isoformat(), current_date.isoformat()))
        current_date += timedelta(days=1)
    
    return extract_stock_movements(source, batch_dates, SCRITP_DIR)

def main():
    """Main updater function"""
    print("🔄 Starting incremental update...")
    
    for source in CONFIG["sicar_sources"]:
        print(f"\n📊 Processing updates for {source['name']}")
        
        # Get last processed timestamp
        last_ts = get_last_processed_timestamp(source['store'])
        
        if last_ts:
            print(f"📅 Last processed timestamp: {last_ts}")
            # Add a small buffer to avoid missing data due to timing issues
            start_ts = last_ts + timedelta(seconds=1)
        else:
            print("⚠️ No checkpoint found, starting from default date")
            start_ts = datetime(2024, 10, 26)
        
        print(f"🚀 Extracting data from {start_ts} onwards...")
        
        # Extract and load new data
        total_rows = 0
        max_fecha = None
        
        try:
            for df in extract_incremental_data(source, start_ts):
                if not df.empty:
                    # Filter out records that are not newer than last_ts
                    if last_ts:
                        df = df[pd.to_datetime(df['fecha']) > last_ts]
                    
                    if not df.empty:
                        # Load to database
                        df.to_sql(
                            "raw_stock_movements", 
                            con=engine, 
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
                print(f"✅ Loaded {total_rows} new rows")
                
                # Update checkpoint with the maximum fecha processed
                if max_fecha:
                    update_last_processed_timestamp(source['store'], max_fecha)
                    print(f"📌 Updated checkpoint to: {max_fecha}")
            else:
                print(f"ℹ️ No new records found for {source['name']}")
                
        except Exception as e:
            print(f"❗️ Error processing {source['name']}: {e}")
            continue
    
    print("\n🎉 Incremental update completed!")

if __name__ == "__main__":
    # Import pandas here since it's used in the main function
    import pandas as pd
    main()