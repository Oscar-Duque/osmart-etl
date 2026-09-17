import json
from pathlib import Path
import pandas as pd
from pandas import to_datetime
import numpy as np
import logging
from sqlalchemy import create_engine, text
from datetime import date, datetime, timedelta
from etl_inventory.stock_points_helpers import verify_stock_accuracy

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH  = PROJECT_ROOT / "config_v2.json"
CONFIG = json.load(open(CONFIG_PATH))

get_last_raw_ts_sql = Path(SCRIPT_DIR / "sql/get_last_raw_ts.sql").read_text(encoding="utf-8")
get_max_prod_raw_ts_sql = Path(SCRIPT_DIR / "sql/get_max_prod_raw_ts.sql").read_text(encoding="utf-8")
get_max_points_dt_sql = Path(SCRIPT_DIR / "sql/get_max_points_dt.sql").read_text(encoding="utf-8")
extract_filter_raw_stock_movements_incr_sql = Path(SCRIPT_DIR / "sql/extract_filter_raw_stock_movements_incremental.sql").read_text(encoding="utf-8")
get_last_sod_stocks_sql = Path(SCRIPT_DIR / "sql/get_last_sod_stocks.sql").read_text(encoding="utf-8")
set_last_points_dt_sql = Path(SCRIPT_DIR / "sql/set_last_points_dt.sql").read_text(encoding="utf-8")

def main():
    # Create connection to the cleaned data database (osmart_data)
    analytics_source = CONFIG["cedis"]["analytics_source"]
    analytics_engine = create_engine(
        f"mysql+pymysql://{analytics_source['user']}:"
        f"{analytics_source['password']}@{analytics_source['host']}:"
        f"{analytics_source['port']}/{analytics_source['database']}"
    )

    stores = ['cedis', 'vallarta', 'renacimiento', 'velazquez', 'coloso', 'zapata']

    for store in  stores:
        store_id = CONFIG[store]["store_id"]
        source = CONFIG[store]["sicar_source"]
        logging.info(f"--- Processing store: {store} ---")
        
        ### verify if raw stock movements is updated ###
        
        yesterday = date.today()- timedelta(days=1)
        prod_engine = create_engine(
            f"mysql+pymysql://{source['user']}:"
            f"{source['password']}@{source['host']}:"
            f"{source['port']}/{source['database']}"
        )

        with prod_engine.begin() as conn:
            max_prod_ts = conn.execute(
                text(get_max_prod_raw_ts_sql),
                {"start_date": yesterday, "end_date": yesterday, "source_id": source["source_id"], "tienda_id": store_id,}
            ).scalar()

        with analytics_engine.begin() as conn:
            last_raw_stock_ts = conn.execute(
                text(get_last_raw_ts_sql),
                {'source_id': source["source_id"]}
            ).scalar()
            
        max_prod_ts = to_datetime(max_prod_ts) if max_prod_ts else None
        last_raw_stock_ts = to_datetime(last_raw_stock_ts) if last_raw_stock_ts else None

        if max_prod_ts is None:
            logging.info("No hay movimientos nuevos en producción")
            continue
        if last_raw_stock_ts is None:
            logging.info("raw_stock_movements está vacía, se debe cargar todo")
        elif last_raw_stock_ts < max_prod_ts:
            logging.info(f"La tabla raw_stock_movements no está actualizada")
            continue
        else:
            logging.info(f"Tabla raw_stock_movements actualizada")

        ### Calculate date range to export ###
        
        with analytics_engine.begin() as conn:
            last_processed_date = conn.execute(
                text(get_max_points_dt_sql),
                {'source_id': source["source_id"]}
            ).scalar()

        if last_processed_date:
            logging.info(f"📅 Last processed timestamp: {last_processed_date}")
            # Add a small buffer to avoid missing data due to timing issues
            movement_start_date = last_processed_date
        else:
            logging.warning("⚠️ No checkpoint found, starting from default date")
            movement_start_date = date(2024, 10, 1)

        movement_end_date = date.today()- timedelta(days=1) # Yesterday - only process complete days
        calendar_end_date = date.today()
        
        if movement_start_date > movement_end_date:
            logging.info(f"✅ No new movement data to process for {store}")
            continue
        
        logging.info(f"📅 Calculating SOD stock from {movement_start_date} up to {movement_end_date}")
        
        ### Extract raw stock movements for the date range ###
        
        with analytics_engine.begin() as conn:
            df = pd.read_sql_query(
                text(extract_filter_raw_stock_movements_incr_sql),
                conn,
                params = {
                    "source_id": source["source_id"],
                    "start_date": movement_start_date.isoformat(),
                    "end_date": movement_end_date.isoformat()
                }
            )
            
        if df.empty:
            logging.info(f"ℹ️ No new raw movements found")
            continue
        
        df.to_csv(PROJECT_ROOT / f"debug/output_{source['source_id']}_{source['source_name']}_raw(1).csv")
        logging.info(f"🔄 Processing {len(df)} raw movements...")
        
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
        
        logging.info(f"Computing SOD stock from last processed date...")
        
        last_sod_stocks_df = pd.Series(dtype='int64')
        with analytics_engine.begin() as conn:
            last_sod_stocks_df = pd.read_sql_query(
                text(get_last_sod_stocks_sql),
                conn,
                params = {'source_id': source["source_id"], 'as_of_date': last_processed_date}
            )
            
        last_sod_stocks_df = last_sod_stocks_df.set_index('art_id')['sod_stock']
        last_sod_stocks_df.to_csv(PROJECT_ROOT / f"debug/output_{source['source_id']}_{source['source_name']}_last(2).csv")
        
        ### Transform raw movements into daily deltas ###
        
        logging.info(f"Transform raw movements into daily deltas...")
        
        out_rows = []

        for art_id, g in df.groupby('art_id', sort=False):
            # Start with the last known SOD stock for this product
            try:
                running = last_sod_stocks_df.loc[art_id]
            except KeyError:
                running = 0

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
        
        logging.info(f"Compute daily net deltas...")
        
        if not out_rows:
            # No movements, return empty DataFrame
            empty = pd.DataFrame(columns=['art_id', 'fecha', 'sod_stock'])
            logging.info(f"No movements")
            continue

        temp = pd.DataFrame(out_rows, columns=['art_id', 'fecha', 'delta_cantidad'])
        daily_net = (temp.groupby(['art_id', 'fecha'], as_index=False)['delta_cantidad']
                        .sum()
                        .sort_values(['art_id', 'fecha']))
        
        # daily_net.to_csv(PROJECT_ROOT / f"debug/output_{source['source_id']}_{source['source_name']}_daily_net(4).csv")
        
        logging.info(f"Create calendar range...")
        
        # Create calendar range
        cal = pd.date_range(pd.to_datetime(movement_start_date).date(),
                        pd.to_datetime(calendar_end_date).date(),
                        freq='D').date
        
        # Get all art_ids (both from movements and from last_sod_stocks)
        movement_art_ids = set(daily_net['art_id'].unique())
        last_stock_art_ids = set(last_sod_stocks_df.keys())
        all_art_ids = movement_art_ids.union(last_stock_art_ids)

        # Pivot to wide format and fill missing dates with 0
        wide = (daily_net.pivot(index='art_id', columns='fecha', values='delta_cantidad')
                    .reindex(index=list(all_art_ids), columns=cal)
                    .fillna(0)
                    .astype(int)).sort_index()
        
        wide.to_csv(PROJECT_ROOT / f"debug/output_{source['source_id']}_{source['source_name']}_wide(5).csv")
        
        # Step 6: Calculate SOD stocks starting from last known SOD stocks
        
        logging.info(f"Calculate SOD stocks starting from last known SOD stocks...")
        
        sod_results = []

        for art_id in wide.index:
            last_sod = last_sod_stocks_df.get(art_id, 0)
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
        
        logging.info(f"Convert to DataFrame in wide format...")
        
        temp_df = pd.DataFrame(sod_results, columns=['art_id', 'fecha', 'sod_stock'])
        start_stock = (temp_df.pivot(index='art_id', columns='fecha', values='sod_stock')
                        .reindex(index=list(all_art_ids), columns=cal)
                            .sort_values('art_id'))
        
        start_stock.to_csv(PROJECT_ROOT / f"debug/output_{source['source_id']}_{source['source_name']}_start_stock(7).csv")

        ### Verify accuracy ###
        
        verify_stock_accuracy(source, store_id, start_stock)
        
        ### Save stock points ###
        
        logging.info(f"💾 Saving stock points...")
        
        # Prepare data
        cols = sorted(start_stock.columns)
        sod = start_stock[cols].fillna(0).astype('int64')
        sod = sod.rename_axis(index='art_id', columns='point_date')
        sod.columns = pd.to_datetime(sod.columns).normalize()
        sod = sod.sort_index(axis=1).astype('int64')
        
        # # Detect change-days (compared to previous day)
        # prev = sod.shift(axis=1)
        # change_mask = prev.isna() | sod.ne(prev)
        
        # Construir la columna "prev" real: último stock guardado para cada art_id
        # (último punto de stock_points antes del rango actual)
        last_known = (
            last_sod_stocks_df
            .reindex(sod.index)
            .fillna(0)
        )
        
        # Shift interno del lote
        prev_internal = sod.shift(axis=1)
        
        # Reemplazar solo la primera columna (NaN) con el stock real conocido
        prev = prev_internal.copy()
        prev.iloc[:, 0] = last_known.values
        
        change_mask = sod.ne(prev)
        
        # Create sparse points
        stacked_vals = sod.stack()
        stacked_mask = change_mask.stack()
        points = stacked_vals[stacked_mask]
        points = points.rename('sod_stock').reset_index()
        
        # Format for database
        points['point_date'] = pd.to_datetime(points['point_date']).dt.date
        points['source_id'] = source['source_id']
        points['tienda_id'] = store_id
        points = points[['source_id','art_id','point_date','sod_stock', 'tienda_id']]
        
        if points.empty:
            logging.info(f"ℹ️ No stock changes detected")
            continue
        
        # Bulk insert via temp table
        with analytics_engine.begin() as conn:
            conn.exec_driver_sql("""
                CREATE TEMPORARY TABLE _init_points (
                source_id   INT NOT NULL,
                art_id     INT NOT NULL,
                point_date DATE NOT NULL,
                sod_stock  BIGINT NOT NULL,
                tienda_id INT NOT NULL,
                PRIMARY KEY (source_id, art_id, point_date)
                ) ENGINE=InnoDB;
            """)
            
            points.to_sql('_init_points', conn, if_exists='append', index=False)
            
            conn.exec_driver_sql("""
                INSERT INTO stock_points (source_id, art_id, point_date, sod_stock, tienda_id)
                SELECT source_id, art_id, point_date, sod_stock, tienda_id FROM _init_points
                ON DUPLICATE KEY UPDATE sod_stock = VALUES(sod_stock);
            """)

            conn.exec_driver_sql("DROP TEMPORARY TABLE _init_points;")
        
        logging.info(f"✅ Saved {len(points)} stock points")
        
        ### Update checkpoint ###
        
        with analytics_engine.begin() as conn:
            max_date = conn.execute(
                text(get_max_points_dt_sql), 
                {'source_id': source['source_id']}
            ).scalar()
                    
            conn.execute(
                text(set_last_points_dt_sql),
                {"dt": max_date, 'source_name': source["source_name"]}
            )
        
        logging.info(f"📌 Updated checkpoint to: {max_date}")
        
    logging.info(f"\n🎉 Stock points incremental update completed!")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()