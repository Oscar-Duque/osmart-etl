import json
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import date, timedelta
from stock_points_helpers import verify_stock_accuracy
from dq_exclusions_csv import apply_exclusions_and_log

SCRITP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parent.parent   # osmart-etl/
CONFIG_PATH  = PROJECT_ROOT / "config.json"
CONFIG = json.load(open(CONFIG_PATH))
EXCLUSIONS_CSV = Path("dq_exclusions.csv")  # choose your path
ABS_MAX = 1_000_000  # tune per business reality

# Create connection to the analytics database (osmart_data)
db_config = CONFIG["analytics_db"]
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Delete and create stock_points table
stock_pints_sql = Path(SCRITP_DIR / "sql/create_stock_points.sql").read_text(encoding="utf-8")
stock_pints_queries = stock_pints_sql.split(';')

with engine.begin() as conn:
    for query in stock_pints_queries:
        if query.strip(): 
            conn.execute(text(query))
            
# Restart etl progress tracker for all stores
reset_last_points_dt_sql = Path(SCRITP_DIR / "sql/reset_last_points_dt.sql").read_text(encoding="utf-8")

with engine.begin() as conn:
    conn.execute(text(reset_last_points_dt_sql))

for source in CONFIG["sicar_sources"]:
    print(f"🚀 Extracting historical raw stock movements data for {source['name']}")
    ## Aggregate raw stock movements into daily net changes per product and store.
    # Extract from raw logs
    with open(SCRITP_DIR / "sql/extract_filter_raw_stock_movements.sql", "r") as f:
        query = text(f.read())

    with engine.begin() as conn:
        df = pd.read_sql_query(query, conn, params={"store_id": source['store_id'],})
        
    # Filter & log exclusions (threshold-based)
    df, flagged = apply_exclusions_and_log(
        df=df,
        store_id=source['store_id'],
        csv_path=EXCLUSIONS_CSV,
        abs_max=ABS_MAX
    )
    if flagged:
        print(f"[DQ] Excluded {flagged} raw rows (manual or absurd absolute snapshots).")
    
    print(f"Cleaning data...")
    # Ensure types
    df['fecha'] = pd.to_datetime(df['fecha'])
    # normalize flags
    df['is_absolute'] = df.get('is_absolute', 0).fillna(0).astype(bool)
    # ensure numeric types
    if 'delta_cantidad' not in df.columns:
        df['delta_cantidad'] = np.nan
    if 'abs_stock_after' not in df.columns:
        df['abs_stock_after'] = np.nan

    # stable chronological order per SKU
    df = df.sort_values(['art_id','fecha'], kind='mergesort')

    print(f"Computing daily net deltas...")
    # transform abs_stock_after into deltas
    out_rows = []
    for art_id, g in df.groupby('art_id', sort=False):
        running = 0  # because your history contains initial loads; first absolute snaps it anyway
        for _, r in g.iterrows():
            if r['is_absolute']:
                target = int(r['abs_stock_after']) if pd.notnull(r['abs_stock_after']) else 0
                d = target - running
                running = target
            else:
                d = int(r['delta_cantidad']) if pd.notnull(r['delta_cantidad']) else 0
                running += d
            out_rows.append((art_id, r['fecha'].date(), d))

    # compute daily net deltas
    temp = pd.DataFrame(out_rows, columns=['art_id','fecha','delta_cantidad'])
    daily_net = (temp.groupby(['art_id','fecha'], as_index=False)['delta_cantidad']
                    .sum()
                    .sort_values(['art_id','fecha']))
    start_date = date(2024, 10, 26)
    end_date = date.today()
    cal = pd.date_range(pd.to_datetime(start_date).date(),
                            pd.to_datetime(end_date).date(),
                            freq='D').date

    # Pivot to wide: rows=art_id, cols=date, values=delta
    wide = (daily_net.pivot(index='art_id', columns='fecha', values='delta_cantidad')
                .reindex(columns=cal)
                .fillna(0)
                .astype(int))

    # Initial stock vector
    eod = wide.cumsum(axis=1)
    start_stock = eod.shift(1, axis=1, fill_value=0).astype('int64')

    ### Verify calculated stock vs actual stock
    verify_stock_accuracy(source, start_stock, SCRITP_DIR)

    ## Load into sparse logs
    # Ensure clean labels & types
    cols = sorted(start_stock.columns)
    sod = start_stock[cols].fillna(0).astype('int64')
    sod = sod.rename_axis(index='art_id', columns='point_date')
    sod.columns = pd.to_datetime(sod.columns).normalize()
    sod = sod.sort_index(axis=1).astype('int64')
    
    # Detect change-days
    prev = sod.shift(axis=1)
    change_mask = prev.isna() | sod.ne(prev)
    
    # Stack first (int), then filter by stacked mask (no NaNs -> stays int)
    stacked_vals = sod.stack()                 # int64
    stacked_mask = change_mask.stack()         # bool
    points = stacked_vals[stacked_mask]        # int64
    points = points.rename('sod_stock').reset_index()  # cols: art_id, point_date, sod_stock

    # point_date as DATE objects for MySQL
    points['point_date'] = pd.to_datetime(points['point_date']).dt.date
    points['store_id'] = source['store_id']
    points = points[['store_id','art_id','point_date','sod_stock']]
    
    INT_MIN, INT_MAX = -(2**31), 2**31 - 1

    # DEBUG: Basic sanity on range
    minv, maxv = int(points['sod_stock'].min()), int(points['sod_stock'].max())
    print(f"[DEBUG] sod_stock min={minv}, max={maxv}, rows={len(points)}")

    bad = points[(points['sod_stock'] < INT_MIN) | (points['sod_stock'] > INT_MAX)]
    if not bad.empty:
        print(f"[ERROR] {len(bad)} rows out of MySQL INT range. Showing a few:")
        print(bad.head(10).to_string(index=False))

        # help trace back: which SKU/dates blow up?
        offenders = (bad.groupby('art_id')['sod_stock']
                    .agg(['min', 'max', 'count'])
                    .sort_values('count', ascending=False))
        print(offenders.head(10))

    # DEBUG: Save data to csv to inspect
    points.to_csv(f"output_{source['store_id']}_{source['store']}_points.csv")

    # 5) bulk-insert via temp table (idempotent)
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
        points[['store_id','art_id','point_date','sod_stock']].to_sql(
            '_init_points', conn, if_exists='append', index=False
        )

        conn.exec_driver_sql("""
            INSERT INTO stock_points (store_id, art_id, point_date, sod_stock)
            SELECT store_id, art_id, point_date, sod_stock FROM _init_points
            ON DUPLICATE KEY UPDATE sod_stock = VALUES(sod_stock);
        """)

        conn.exec_driver_sql("DROP TEMPORARY TABLE _init_points;")
    
    # 6) Set last_points_dt to the max date_time of the data inserted
    get_max_points_dt_sql = Path(SCRITP_DIR / "sql/get_max_points_dt.sql").read_text(encoding="utf-8")
    set_last_points_dt_sql = Path(SCRITP_DIR / "sql/set_last_points_dt.sql").read_text(encoding="utf-8")

    with engine.begin() as conn:
        max_dt = conn.execute(
            text(get_max_points_dt_sql), 
            {'store_id': source['store_id']}
        ).scalar()
        
        conn.execute(
            text(set_last_points_dt_sql),
            {"dt": max_dt, 'store_name': source['store']}
        )