import json
from pathlib import Path
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from etl_sales.db_helpers import insert_on_conflict_update

SCRITP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parent.parent   # osmart-etl/
CONFIG_PATH  = PROJECT_ROOT / "config_v2.json"
CONFIG = json.load(open(CONFIG_PATH))

def main():
    # Create connection to the cleaned data database (osmart_data)
    analytics_source = CONFIG["cedis"]["analytics_source"]
    analytics_engine = create_engine(
        f"mysql+pymysql://{analytics_source['user']}:"
        f"{analytics_source['password']}@{analytics_source['host']}:"
        f"{analytics_source['port']}/{analytics_source['database']}"
    )

    stores = ['vallarta', 'renacimiento', 'velazquez', 'coloso', 'zapata']

    # For each SICAR source (store)
    for store in stores:
        store_id = CONFIG[store]["store_id"]
        source = CONFIG[store]["sicar_source"]
        logging.info(f"--- Processing store: {store} ---")
        
        # Get last processed ven_id
        try:
            with analytics_engine.connect() as conn:
                result = conn.execute(
                    text("SELECT MAX(ven_id) FROM ventas_limpias WHERE source_name = :source_name AND source_system = 'sicar'"),
                    {"source_name": source['source_name']}
                ).fetchone()
                last_processed_id = result[0] if result else 0
                logging.info(f"Last processed ven_id: {last_processed_id}")
        except Exception as e:
            logging.error(f"❗️ Error extracting from analytics_db: {e}")
            continue
        
        # Extract sales data where ven_id > last_processed_id
        try:
            # Create source DB connection
            source_engine = create_engine(
                f"mysql+pymysql://{source['user']}:{source['password']}@{source['host']}:{source['port']}/{source['database']}"
            )
            
            # Extract new sales
            with source_engine.connect() as conn:
                extract_latest_sicar_sales_sql = (SCRITP_DIR / "db/extract_latest_sicar_sales.sql").read_text(encoding="utf-8")
                
                logging.info(f"🔄 Extracting SICAR sales for {store}")
                df = pd.read_sql_query(
                    text(extract_latest_sicar_sales_sql),
                    conn,
                    params={"last_id": last_processed_id}
                )

                if df.empty:
                    logging.info("No new sales found.")
                    continue

                logging.info(f"Found {len(df)} new sales.")
        
        except Exception as e:
            logging.error(f"❗️ Error extracting for {store}: {e}")
            continue
        
        # Transform data
        df["source_name"] = source["source_name"]
        df["source_db"] = source["database"]
        df["source_system"] = "sicar"
        df["extracted_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        df["tienda_id"] = store_id
        
        # Load into ventas_limpias and update etl_progress
        try:
            with analytics_engine.begin() as conn:
                df.to_sql(
                    "ventas_limpias", 
                    con=conn, 
                    if_exists="append", 
                    index=False,
                    method=insert_on_conflict_update
                )

                max_ven_id = df["ven_id"].max()
                conn.execute(
                    text("""
                        UPDATE etl_progress
                        SET last_processed_ven_id = :last_id
                        WHERE source_name = :source_name
                    """),
                    {"source_name": source['source_name'], "last_id": max_ven_id}
                )
                
                logging.info(f"Finished {store}. Last ven_id now {max_ven_id}.")
        
        except Exception as e:
            logging.error(f"❗️ Error inserting data for {store}: {e}")
            continue
        
    logging.info("\nAll stores processed.")
    
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()