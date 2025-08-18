import json
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from db.db_helpers import insert_on_conflict_update

# Setup logging to file + console
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# File handler
file_handler = logging.FileHandler("logs/update_clean_data.log")
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

# Root logger config
logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

CONFIG = json.load(open("../config.json"))

# Create connection to the cleaned data database (osmart_data)
db_config = CONFIG["analytics_db"]
analytics_engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# For each SICAR source (store)
for source in CONFIG["sicar_sources"]:
    store_name = source["store"]
    logging.info(f"\n--- Processing store: {store_name} ---")

    # Get last processed ven_id
    try:
        with analytics_engine.connect() as conn:
            result = conn.execute(
                text("SELECT last_processed_ven_id FROM etl_progress WHERE store_name = :store"),
                {"store": store_name}
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
            with open("db/extract_latest_sicar_sales.sql", "r") as f:
                    query =  text(f.read())
            
            logging.info(f"🔄 Extracting SICAR sales for {source['store']}")
            df = pd.read_sql_query(
                query,
                conn,
                params={"last_id": last_processed_id}
            )

            if df.empty:
                logging.info("No new sales found.")
                continue

            logging.info(f"Found {len(df)} new sales.")
    
    except Exception as e:
        logging.error(f"❗️ Error extracting for {source['store']}: {e}")
        continue
    
    # Transform data
    df["tienda"] = source["store"]
    df["source_db"] = source["database"]
    df["source_system"] = "sicar"
    df["extracted_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    
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
            # import pdb; pdb.set_trace()
            conn.execute(
                text("""
                    UPDATE etl_progress
                    SET last_processed_ven_id = :last_id
                    WHERE store_name = :store
                """),
                {"store": store_name, "last_id": max_ven_id}
            )
            
            logging.info(f"Finished {store_name}. Last ven_id now {max_ven_id}.")
    
    except Exception as e:
        logging.error(f"❗️ Error inserting data for {store_name}: {e}")
        continue
    
logging.info("\nAll stores processed.")