import json
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import date, timedelta
from stock_points_helpers import verify_stock_accuracy

SCRIPT_DIR = Path(__file__).resolve().parent

PROJECT_ROOT = Path(__file__).resolve().parent.parent   # osmart-etl/
CONFIG_PATH  = PROJECT_ROOT / "config.json"
CONFIG = json.load(open(CONFIG_PATH))

# Create connection to the cleaned data database (osmart_data)
db_config = CONFIG["analytics_db"]
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

def get_last_processed_date(store_name):
    """Get the last processed date for stock points"""
    get_last_dt_sql = Path(SCRIPT_DIR / "sql/get_last_points_dt.sql").read_text(encoding="utf-8")
    
    with engine.begin() as conn:
        result = conn.execute(
            text(get_last_dt_sql),
            {'store_name': store_name}
        ).scalar()
        
        return result

def update_last_processed_date(store_name, dt):
    """Update the last processed date for stock points"""
    set_last_dt_sql = Path(SCRIPT_DIR / "sql/set_last_points_dt.sql").read_text(encoding="utf-8")
    
    with engine.begin() as conn:
        conn.execute(
            text(set_last_dt_sql),
            {"dt": dt, 'store_name': store_name}
        )

def get_existing_stock_data(store_id, as_of_date):
    """Get existing stock data from the last known date"""
    with engine.begin() as conn:
        # Get the stock on the last known date for all products
        existing_stock_sql = text("""
            WITH target AS (SELECT COALESCE(:as_of_date, MAX(point_date)) AS as_of_date FROM stock_points WHERE store_id = :store_id),
            ranked AS (
              SELECT
                sp.art_id,
                sp.point_date,
                sp.sod_stock,
                ROW_NUMBER() OVER (PARTITION BY sp.art_id ORDER BY sp.point_date DESC, sp.updated_at DESC) AS rn
              FROM
                stock_points sp
                JOIN target t ON sp.point_date <= t.as_of_date
              WHERE
                sp.store_id = :store_id
            ) SELECT
              art_id,
              sod_stock
            FROM
              ranked
            WHERE
              rn = 1
            ORDER BY
              art_id;
        """)
        
        existing = pd.read_sql_query(
            existing_stock_sql, 
            conn, 
            params={"store_id": store_id, "as_of_date": as_of_date}
        )
        
        return existing.set_index('art_id')['sod_stock']

def process_incremental_update(source, last_processed_date):
    """Process incremental stock movements and update stock points"""
    
    # Calculate date range for processing
    # For movements: include the checkpoint date since it only represents start-of-day
    # For calendar: go up to today to calculate today's starting stock
    movement_start_date = last_processed_date if last_processed_date else date(2024, 10, 26)
    movement_end_date = date.today() - timedelta(days=1)  # Yesterday - only process complete days
    calendar_end_date = date.today()  # Calendar goes to today for SOD calculation
    
    if movement_start_date > movement_end_date:
        print(f"✅ No new movement data to process for {source['name']}")
        return None
    
    print(f"📅 Processing movements from {movement_start_date} to {movement_end_date}")
    print(f"📅 Calculating SOD stock up to {calendar_end_date}")
    
    # Extract raw stock movements for the date range
    with open(SCRIPT_DIR / "sql/extract_filter_raw_stock_movements_incremental.sql", "r") as f:
        query = text(f.read())

    with engine.begin() as conn:
        df = pd.read_sql_query(
            query, 
            conn, 
            params={
                "store_id": source['store_id'],
                "start_date": movement_start_date.isoformat(),
                "end_date": movement_end_date.isoformat()
            }
        )

    if df.empty:
        print(f"ℹ️ No new raw movements found")

    print(f"🔄 Processing {len(df)} raw movements...")
    
    # Clean and prepare data
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['is_absolute'] = df.get('is_absolute', 0).fillna(0).astype(bool)

    if 'delta_cantidad' not in df.columns:
        df['delta_cantidad'] = np.nan
    if 'abs_stock_after' not in df.columns:
        df['abs_stock_after'] = np.nan

    df = df.sort_values(['art_id','fecha'], kind='mergesort')
    
    # Get SOD stock from last processed date
    last_sod_stocks = pd.Series(dtype='int64')
    if last_processed_date:
        last_sod_stocks = get_existing_stock_data(source['store_id'], last_processed_date)

    # Step 1: Transform raw movements into daily deltas
    out_rows = []

    for art_id, g in df.groupby('art_id', sort=False):
        # Start with the last known SOD stock for this product
        running = last_sod_stocks.get(art_id, 0)

        for _, r in g.iterrows():
            if r['is_absolute']:
                target = int(r['abs_stock_after']) if pd.notnull(r['abs_stock_after']) else 0
                d = target - running
                running = target
            else:
                d = int(r['delta_cantidad']) if pd.notnull(r['delta_cantidad']) else 0
                running += d
            out_rows.append((art_id, r['fecha'].date(), d))

    # Step 2: Compute daily net deltas
    if not out_rows:
        # No movements, return empty DataFrame
        empty = pd.DataFrame(columns=['art_id', 'fecha', 'sod_stock'])
        return empty, calendar_end_date

    temp = pd.DataFrame(out_rows, columns=['art_id', 'fecha', 'delta_cantidad'])
    daily_net = (temp.groupby(['art_id', 'fecha'], as_index=False)['delta_cantidad']
                    .sum()
                    .sort_values(['art_id', 'fecha']))

    # Step 3: Create calendar range
    cal = pd.date_range(pd.to_datetime(movement_start_date).date(),
                    pd.to_datetime(calendar_end_date).date(),
                    freq='D').date

    # Step 4: Get all art_ids (both from movements and from last_sod_stocks)
    movement_art_ids = set(daily_net['art_id'].unique())
    last_stock_art_ids = set(last_sod_stocks.keys())
    all_art_ids = movement_art_ids.union(last_stock_art_ids)

    # Step 5: Pivot to wide format and fill missing dates with 0
    wide = (daily_net.pivot(index='art_id', columns='fecha', values='delta_cantidad')
                .reindex(index=list(all_art_ids), columns=cal)
                .fillna(0)
                .astype(int)).sort_index()

    # Step 6: Calculate SOD stocks starting from last known SOD stocks
    sod_results = []

    for art_id in wide.index:
        last_sod = last_sod_stocks.get(art_id, 0)
        running_stock = last_sod

        for fecha in wide.columns:
            # SOD stock is the stock at start of day (before any movements)
            sod_stock = running_stock

            # Apply the day's delta to get EOD stock (which becomes next day's SOD)
            delta = wide.loc[art_id, fecha]
            running_stock += delta

            # Save all stock points (not sparse)
            sod_results.append((art_id, fecha, sod_stock))

    # Step 7: Convert to DataFrame in wide format (like the original 'wide' shape)
    temp_df = pd.DataFrame(sod_results, columns=['art_id', 'fecha', 'sod_stock'])
    result_df = (temp_df.pivot(index='art_id', columns='fecha', values='sod_stock')
                    .reindex(index=list(all_art_ids), columns=cal)
                        .sort_values('art_id'))

    # # Step 7: Convert to DataFrame and sort
    # result_df = pd.DataFrame(sod_results, columns=['art_id', 'fecha', 'sod_stock'])
    # result_df = result_df.sort_values(['art_id', 'fecha']).reset_index(drop=True)

    return result_df, calendar_end_date

def save_stock_points(source, start_stock):
    """Save stock points to database (sparse format)"""
    print(f"💾 Saving stock points...")
    
    # Prepare data
    cols = sorted(start_stock.columns)
    sod = start_stock[cols].fillna(0).astype('int64')
    sod = sod.rename_axis(index='art_id', columns='point_date')
    sod.columns = pd.to_datetime(sod.columns).normalize()
    sod = sod.sort_index(axis=1).astype('int64')
    
    # Detect change-days (compared to previous day)
    prev = sod.shift(axis=1)
    change_mask = prev.isna() | sod.ne(prev)
    
    # Create sparse points
    stacked_vals = sod.stack()
    stacked_mask = change_mask.stack()
    points = stacked_vals[stacked_mask]
    points = points.rename('sod_stock').reset_index()
    
    # Format for database
    points['point_date'] = pd.to_datetime(points['point_date']).dt.date
    points['store_id'] = source['store_id']
    points = points[['store_id','art_id','point_date','sod_stock']]
    
    if points.empty:
        print(f"ℹ️ No stock changes detected")
        return
    
    # Bulk insert via temp table
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TEMPORARY TABLE _init_points (
              store_id   INT NOT NULL,
              art_id     INT NOT NULL,
              point_date DATE NOT NULL,
              sod_stock  BIGINT NOT NULL,
              PRIMARY KEY (store_id, art_id, point_date)
            ) ENGINE=InnoDB;
        """)
        
        points.to_sql('_init_points', conn, if_exists='append', index=False)
        
        conn.exec_driver_sql("""
            INSERT INTO stock_points (store_id, art_id, point_date, sod_stock)
            SELECT store_id, art_id, point_date, sod_stock FROM _init_points
            ON DUPLICATE KEY UPDATE sod_stock = VALUES(sod_stock);
        """)

        conn.exec_driver_sql("DROP TEMPORARY TABLE _init_points;")
    
    print(f"✅ Saved {len(points)} stock points")

def main():
    """Main updater function"""
    print("🔄 Starting stock points incremental update...")
    
    for source in CONFIG["sicar_sources"]:
        print(f"\n📊 Processing stock points for {source['name']}")
        
        # Get last processed date
        last_date = get_last_processed_date(source['store'])
        
        if last_date:
            print(f"📅 Last processed date: {last_date}")
        else:
            print("⚠️ No checkpoint found, starting from scratch")
        
        try:
            # Process incremental data
            result = process_incremental_update(source, last_date)
            
            if result is None:
                continue
                
            start_stock, max_date = result

            # Verify accuracy (only for today)
            verify_stock_accuracy(source, start_stock, SCRIPT_DIR)
            
            # Save stock points
            save_stock_points(source, start_stock)
            
            # Update checkpoint
            update_last_processed_date(source['store'], max_date)
            print(f"📌 Updated checkpoint to: {max_date}")
            
        except Exception as e:
            print(f"❗️ Error processing {source['name']}: {e}")
            continue
    
    print("\n🎉 Stock points incremental update completed!")

if __name__ == "__main__":
    main()