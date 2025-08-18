import json
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import date, timedelta

CONFIG = json.load(open("../config_testing.json"))

# Create connection to the cleaned data database (osmart_data)
db_config = CONFIG["analytics_db"]
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

def get_last_processed_date(store_name):
    """Get the last processed date for stock points"""
    get_last_dt_sql = Path("sql/get_last_points_dt.sql").read_text(encoding="utf-8")
    
    with engine.begin() as conn:
        result = conn.execute(
            text(get_last_dt_sql),
            {'store_name': store_name}
        ).scalar()
        
        return result

def update_last_processed_date(store_name, dt):
    """Update the last processed date for stock points"""
    set_last_dt_sql = Path("sql/set_last_points_dt.sql").read_text(encoding="utf-8")
    
    with engine.begin() as conn:
        conn.execute(
            text(set_last_dt_sql),
            {"dt": dt, 'store_name': store_name}
        )

def get_existing_stock_data(store_id, from_date):
    """Get existing stock data from the last known date"""
    with engine.begin() as conn:
        # Get the stock on the last known date for all products
        existing_stock_sql = text("""
            SELECT art_id, stock_open
            FROM stock_points 
            WHERE store_id = :store_id AND dt = :from_date
        """)
        
        existing = pd.read_sql_query(
            existing_stock_sql, 
            conn, 
            params={"store_id": store_id, "from_date": from_date}
        )
        
        return existing.set_index('art_id')['stock_open']

def process_incremental_update(source, last_processed_date):
    """Process incremental stock movements and update stock points"""
    
    # Calculate date range for processing
    start_date = last_processed_date + timedelta(days=1) if last_processed_date else date(2024, 10, 26)
    end_date = date.today()
    
    if start_date > end_date:
        print(f"✅ No new dates to process for {source['name']}")
        return None
    
    print(f"📅 Processing dates {start_date} to {end_date}")
    
    # Extract raw stock movements for the date range
    with open("sql/extract_filter_raw_stock_movemnts.sql", "r") as f:
        query = text(f.read())

    with engine.begin() as conn:
        df = pd.read_sql_query(
            query, 
            conn, 
            params={
                "store_id": source['store_id'],
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        )
    
    if df.empty:
        print(f"ℹ️ No new raw movements found")
        return None
    
    print(f"🔄 Processing {len(df)} raw movements...")
    
    # Clean and prepare data
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['is_absolute'] = df.get('is_absolute', 0).fillna(0).astype(bool)
    
    if 'delta_cantidad' not in df.columns:
        df['delta_cantidad'] = np.nan
    if 'abs_stock_after' not in df.columns:
        df['abs_stock_after'] = np.nan

    df = df.sort_values(['art_id','fecha'], kind='mergesort')

    # Get starting stock from last processed date
    starting_stock = pd.Series(dtype='int64')
    if last_processed_date:
        starting_stock = get_existing_stock_data(source['store_id'], last_processed_date)
    
    # Transform movements into deltas
    out_rows = []
    for art_id, g in df.groupby('art_id', sort=False):
        # Start with last known stock for this product
        running = int(starting_stock.get(art_id, 0))
        
        for _, r in g.iterrows():
            if r['is_absolute']:
                target = int(r['abs_stock_after']) if pd.notnull(r['abs_stock_after']) else 0
                d = target - running
                running = target
            else:
                d = int(r['delta_cantidad']) if pd.notnull(r['delta_cantidad']) else 0
                running += d
            out_rows.append((art_id, r['fecha'].date(), d))

    # Compute daily net deltas
    temp = pd.DataFrame(out_rows, columns=['art_id','fecha','delta_cantidad'])
    daily_net = (temp.groupby(['art_id','fecha'], as_index=False)['delta_cantidad']
                    .sum()
                    .sort_values(['art_id','fecha']))
    
    # Create calendar for the processing period
    cal = pd.date_range(start_date, end_date, freq='D').date
    
    # Pivot to wide format
    wide = (daily_net.pivot(index='art_id', columns='fecha', values='delta_cantidad')
                .reindex(columns=cal)
                .fillna(0)
                .astype(int))
    
    # Calculate start-of-day stock
    # For incremental updates, we need to add the ending stock from last processed date
    if last_processed_date and not starting_stock.empty:
        # Add starting stock as day 0
        prev_day_col = pd.to_datetime(last_processed_date).date()
        wide.insert(0, prev_day_col, 0)  # Insert dummy column
        
        # Set the starting stock values
        for art_id in wide.index:
            if art_id in starting_stock.index:
                wide.loc[art_id, prev_day_col] = starting_stock.loc[art_id]
        
        # Calculate cumulative sum and shift
        eod = wide.cumsum(axis=1)
        start_stock = eod.shift(1, axis=1, fill_value=0).astype('int64')
        
        # Remove the dummy column
        start_stock = start_stock.drop(columns=[prev_day_col])
    else:
        # First time processing - no previous stock
        eod = wide.cumsum(axis=1)
        start_stock = eod.shift(1, axis=1, fill_value=0).astype('int64')
    
    return start_stock, end_date

def verify_stock_accuracy(source, calculated_stock, target_date):
    """Verify calculated stock against production for the target date"""
    print(f"🔍 Verifying stock accuracy for {target_date}...")
    
    today = pd.Timestamp.now(tz="America/Mexico_City").normalize()
    
    # Only verify if target_date is today
    if target_date != today.date():
        print(f"ℹ️ Skipping verification (not today)")
        return
    
    tomorrow = (today + pd.Timedelta(days=1))
    
    # Connect to production
    prod_engine = create_engine(
        f"mysql+pymysql://{source['user']}:{source['password']}@{source['host']}:{source['port']}/{source['database']}"
    )

    # Get current stock and today's movements
    sql_stock_now = text("SELECT a.art_id, a.existencia AS stock_actual FROM articulo a;")
    
    with open("sql/extract_stock_movements.sql", "r") as f:
        sql_today_events = text(f.read())

    with prod_engine.begin() as conn:
        prod_now = pd.read_sql_query(sql_stock_now, conn)
        today_events = pd.read_sql_query(sql_today_events, conn, params={
            "start_date": today.tz_localize(None), 
            "end_date": tomorrow.tz_localize(None)
        })
    
    # Process today's events to simulate current stock
    ev = today_events.copy()
    if not ev.empty:
        ev['fecha'] = pd.to_datetime(ev['fecha'])
        ev['is_absolute'] = ev['is_absolute'].fillna(0).astype(bool)
        ev['delta_cantidad'] = pd.to_numeric(ev['delta_cantidad'], errors='coerce').fillna(0)
        ev['abs_stock_after'] = pd.to_numeric(ev['abs_stock_after'], errors='coerce')
        ev = ev.sort_values(['art_id','fecha'], kind='mergesort')
    
    # Get calculated start-of-day stock
    start_stock_today = calculated_stock.loc[:, target_date]
    if isinstance(start_stock_today, pd.DataFrame):
        start_stock_today = start_stock_today.iloc[:, 0]
    
    # Simulate to now
    sim_rows = []
    for art_id, g in ev.groupby('art_id', sort=False):
        running = int(start_stock_today.get(art_id, 0))
        for _, r in g.iterrows():
            if r['is_absolute']:
                running = int(r['abs_stock_after']) if pd.notnull(r['abs_stock_after']) else 0
            else:
                running += int(r['delta_cantidad'])
        sim_rows.append((art_id, running))
    
    sim = pd.DataFrame(sim_rows, columns=['art_id','stock_sim_now'])
    
    # Add SKUs with no events today
    missing_today = start_stock_today.index.difference(ev['art_id'].unique() if not ev.empty else [])
    if len(missing_today) > 0:
        missing_df = pd.DataFrame({
            'art_id': missing_today, 
            'stock_sim_now': start_stock_today.loc[missing_today].astype(int).values
        })
        sim = pd.concat([sim, missing_df], ignore_index=True)
    
    # Compare
    comp = (prod_now[['art_id','stock_actual']]
            .merge(sim, on='art_id', how='outer')
            .fillna({'stock_actual':0, 'stock_sim_now':0}))
    comp['diff'] = comp['stock_sim_now'].astype(int) - comp['stock_actual'].astype(int)
    
    summary = {
        'total_skus': len(comp),
        'mismatch_skus': int((comp['diff'] != 0).sum()),
        'max_abs_diff': int(comp['diff'].abs().max()) if not comp.empty else 0
    }
    print(f"📊 Verification: {summary}")

def save_stock_points(source, start_stock):
    """Save stock points to database (sparse format)"""
    print(f"💾 Saving stock points...")
    
    # Prepare data
    cols = sorted(start_stock.columns)
    sod = start_stock[cols].fillna(0).astype('int64')
    sod = sod.rename_axis(index='art_id', columns='dt')
    sod.columns = pd.to_datetime(sod.columns).normalize()
    sod = sod.sort_index(axis=1).astype('int64')
    
    # Detect change-days (compared to previous day)
    prev = sod.shift(axis=1)
    change_mask = prev.isna() | sod.ne(prev)
    
    # Create sparse points
    stacked_vals = sod.stack()
    stacked_mask = change_mask.stack()
    points = stacked_vals[stacked_mask]
    points = points.rename('stock_open').reset_index()
    
    # Format for database
    points['dt'] = pd.to_datetime(points['dt']).dt.date
    points['store_id'] = source['store_id']
    points = points[['store_id','art_id','dt','stock_open']]
    
    if points.empty:
        print(f"ℹ️ No stock changes detected")
        return
    
    # Bulk insert via temp table
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TEMPORARY TABLE _update_points (
              store_id   INT NOT NULL,
              art_id     INT NOT NULL,
              dt         DATE NOT NULL,
              stock_open INT NOT NULL,
              PRIMARY KEY (store_id, art_id, dt)
            ) ENGINE=InnoDB;
        """)
        
        points.to_sql('_update_points', conn, if_exists='append', index=False)
        
        conn.exec_driver_sql("""
            INSERT INTO stock_points (store_id, art_id, dt, stock_open)
            SELECT store_id, art_id, dt, stock_open FROM _update_points
            ON DUPLICATE KEY UPDATE stock_open = VALUES(stock_open);
        """)
        
        conn.exec_driver_sql("DROP TEMPORARY TABLE _update_points;")
    
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
            verify_stock_accuracy(source, start_stock, max_date)
            
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