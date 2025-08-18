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

# Delete and create stock_points table
stock_pints_sql = Path("sql/create_stock_points.sql").read_text(encoding="utf-8")
stock_pints_queries = stock_pints_sql.split(';')

with engine.begin() as conn:
    for query in stock_pints_queries:
        if query.strip(): 
            conn.execute(text(query))
            
# Restart etl progress tracker for all stores
reset_last_points_dt_sql = Path("sql/reset_last_points_dt.sql").read_text(encoding="utf-8")

with engine.begin() as conn:
    conn.execute(text(reset_last_points_dt_sql))

for source in CONFIG["sicar_sources"]:
    print(f"🚀 Extracting historical raw stock movements data for {source['name']}")
    ## Aggregate raw stock movements into daily net changes per product and store.
    # Extract from raw logs
    with open("sql/extract_filter_raw_stock_movemnts.sql", "r") as f:
        query = text(f.read())

    with engine.begin() as conn:
        df = pd.read_sql_query(query, conn, params={"store_id": source['store_id'],})
    
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
    ## Get current stock now and today's net movement from production
    print(f"Extracting updated data to verify calculated stock for today vs actual stock...")
    today = pd.Timestamp.now(tz="America/Mexico_City").normalize()
    tomorrow = (today + pd.Timedelta(days=1))

    # Connect to production db
    # db_config = CONFIG["sicar_sources"][0]
    prod_engine = create_engine(
        f"mysql+pymysql://{source['user']}:{source['password']}@{source['host']}:{source['port']}/{source['database']}"
    )

    # a) current stock now from production
    sql_stock_now = text("""
        SELECT a.art_id, a.existencia AS stock_actual
        FROM articulo a;
    """)

    # b) today's movements
    with open("sql/extract_stock_movements.sql", "r") as f:
        sql_today_events = text(f.read())

    with prod_engine.begin() as conn:
        prod_now = pd.read_sql_query(sql_stock_now, conn)
        today_events = pd.read_sql_query(sql_today_events, conn, params={
            "start_date": today.tz_localize(None), 
            "end_date": tomorrow.tz_localize(None)
        })
        
    print(f"Comparing calculated stock for today vs actual stock...")
    # Ensure types
    ev = today_events.copy()
    ev['fecha'] = pd.to_datetime(ev['fecha'])
    ev['is_absolute'] = ev['is_absolute'].fillna(0).astype(bool)
    ev['delta_cantidad'] = pd.to_numeric(ev['delta_cantidad'], errors='coerce').fillna(0)
    ev['abs_stock_after'] = pd.to_numeric(ev['abs_stock_after'], errors='coerce')
    ev = ev.sort_values(['art_id','fecha'], kind='mergesort')

    start_stock_today_calc = start_stock.loc[:, today.date()]

    # Start-of-day calc as a Series
    sod = start_stock_today_calc.copy()
    if isinstance(sod, pd.DataFrame):
        sod = sod.iloc[:, 0]  # ensure Series

    # Simulate to NOW
    sim_rows = []
    for art_id, g in ev.groupby('art_id', sort=False):
        running = int(sod.get(art_id, 0))  # default 0 if SKU not present
        for _, r in g.iterrows():
            if r['is_absolute']:
                running = int(r['abs_stock_after']) if pd.notnull(r['abs_stock_after']) else 0
            else:
                running += int(r['delta_cantidad'])
        sim_rows.append((art_id, running))
    sim = pd.DataFrame(sim_rows, columns=['art_id','stock_sim_now'])

    # SKUs with no events today: simulated NOW = start-of-day
    missing_today = sod.index.difference(ev['art_id'].unique())
    if len(missing_today) > 0:
        sim = pd.concat([sim, pd.DataFrame({'art_id': missing_today, 'stock_sim_now': sod.loc[missing_today].astype(int).values})],
                        ignore_index=True)
        
    # Compare to production
    comp = (prod_now[['art_id','stock_actual']]
            .merge(sim, on='art_id', how='outer')
            .fillna({'stock_actual':0, 'stock_sim_now':0}))
    comp['diff'] = comp['stock_sim_now'].astype(int) - comp['stock_actual'].astype(int)
    comp.to_csv(f"output_{source['store_id']}_{source['store']}.csv")

    summary = {
        'total_skus': len(comp),
        'mismatch_skus': int((comp['diff'] != 0).sum()),
        'max_abs_diff': int(comp['diff'].abs().max()) if not comp.empty else 0
    }
    print(summary)

    ## Load into sparse logs
    # Ensure clean labels & types
    cols = sorted(start_stock.columns)
    sod = start_stock[cols].fillna(0).astype('int64')
    sod = sod.rename_axis(index='art_id', columns='dt')
    sod.columns = pd.to_datetime(sod.columns).normalize()
    sod = sod.sort_index(axis=1).astype('int64')
    
    # Detect change-days
    prev = sod.shift(axis=1)
    change_mask = prev.isna() | sod.ne(prev)
    
    # Stack first (int), then filter by stacked mask (no NaNs -> stays int)
    stacked_vals = sod.stack()                 # int64
    stacked_mask = change_mask.stack()         # bool
    points = stacked_vals[stacked_mask]        # int64
    points = points.rename('stock_open').reset_index()  # cols: art_id, dt, stock_open

    # dt as DATE objects for MySQL
    points['dt'] = pd.to_datetime(points['dt']).dt.date
    points['store_id'] = source['store_id']
    points = points[['store_id','art_id','dt','stock_open']]

    # 5) bulk-insert via temp table (idempotent)
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TEMPORARY TABLE _init_points (
              store_id   INT NOT NULL,
              art_id     INT NOT NULL,
              dt         DATE NOT NULL,
              stock_open INT NOT NULL,
              PRIMARY KEY (store_id, art_id, dt)
            ) ENGINE=InnoDB;
        """)
        points[['store_id','art_id','dt','stock_open']].to_sql(
            '_init_points', conn, if_exists='append', index=False
        )

        conn.exec_driver_sql("""
            INSERT INTO stock_points (store_id, art_id, dt, stock_open)
            SELECT store_id, art_id, dt, stock_open FROM _init_points
            ON DUPLICATE KEY UPDATE stock_open = VALUES(stock_open);
        """)
        conn.exec_driver_sql("DROP TEMPORARY TABLE _init_points;")
    
    # 6) Set last_points_dt to the max date_time of the data inserted
    get_max_points_dt_sql = Path("sql/get_max_points_dt.sql").read_text(encoding="utf-8")
    set_last_points_dt_sql = Path("sql/set_last_points_dt.sql").read_text(encoding="utf-8")

    with engine.begin() as conn:
        max_dt = conn.execute(
            text(get_max_points_dt_sql), 
            {'store_id': source['store_id']}
        ).scalar()
        
        conn.execute(
            text(set_last_points_dt_sql),
            {"dt": max_dt, 'store_name': source['store']}
        )