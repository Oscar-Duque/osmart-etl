import pandas as pd
from sqlalchemy import create_engine, text
    
def verify_stock_accuracy(source, calculated_stock, script_dir):
    ## Get current stock now and today's net movement from production
    print(f"🔍 Verifying stock accuracy")
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
    with open(script_dir / "sql/extract_stock_movements.sql", "r") as f:
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

    start_stock_today_calc = calculated_stock.loc[:, today.date()]

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
    print(f"📊 Verification: {summary}")