import json
from datetime import date, timedelta
import calendar
from sqlalchemy import create_engine, text
from pathlib import Path
from extract import extract_stock_movements

CONFIG = json.load(open("../config_testing.json"))

# Create connection to the cleaned data database (osmart_data)
db_config = CONFIG["analytics_db"]
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Delete and create raw_stock_movements table
raw_stock_movements_sql = Path("sql/create_raw_stock_movements.sql").read_text(encoding="utf-8")
raw_stock_movements_queries = raw_stock_movements_sql.split(';')

with engine.begin() as conn:
    for query in raw_stock_movements_queries:
        if query.strip(): 
            conn.execute(text(query))

# Restart etl progress tracker for all stores
reset_last_raw_ts_sql = Path("sql/reset_last_raw_ts.sql").read_text(encoding="utf-8")

with engine.begin() as conn:
    conn.execute(text(reset_last_raw_ts_sql))

for source in CONFIG["sicar_sources"]:
    # 1. Extract
    print(f"🚀 Extracting historical data for {source['name']}")
    
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

    for df in extract_stock_movements(source, batch_dates):
        # 2. Load raw logs (for audit/debug)
        df.to_sql(
            "raw_stock_movements", 
            con=engine, 
            if_exists="append", 
            index=False,
            method="multi"
        )

    # 3. Set last_raw_ts to max 'fecha'
    get_max_raw_ts_sql = Path("sql/get_max_raw_ts.sql").read_text(encoding="utf-8")
    set_last_raw_ts_sql = Path("sql/set_last_raw_ts.sql").read_text(encoding="utf-8")
        
    with engine.begin() as conn:
        max_fecha = conn.execute(
            text(get_max_raw_ts_sql), 
            {'tienda_id': source['store_id']}
        ).scalar()
        
        conn.execute(
            text(set_last_raw_ts_sql),
            {"ts": max_fecha, 'store_name': source['store']}
        )