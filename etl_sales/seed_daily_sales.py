import json
from pathlib import Path
import logging
from sqlalchemy import create_engine, text

SCRITP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCES = json.load(open(PROJECT_ROOT / "sources.json"))
LOG_PATH = PROJECT_ROOT / "logs/update_clean_data.log"

create_fact_daily_sales_sql = Path(SCRITP_DIR / "db/create_fact_daily_sales.sql").read_text(encoding="utf-8")
extract_insert_daily_sales_sql = Path(SCRITP_DIR / "db/extract_insert_daily_sales.sql").read_text(encoding="utf-8")

def main():
    # Connect to analytics database
    
    logging.info(f"Connecting to analytics database")
    
    source = SOURCES["cedis_analytics"]
    analytics_engine = create_engine(
        f"mysql+pymysql://{source['user']}:"
        f"{source['password']}@{source['host']}:"
        f"{source['port']}/{source['database']}"
    )

    # Delete and create fact_daily_sales table
    
    logging.info(f"Creating fact_daily_sales table")

    with analytics_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS `fact_daily_sales`;"))
        conn.execute(text(create_fact_daily_sales_sql))

    # Seed fact_daily_sales with INSERT and SELECT query
    
    logging.info(f"Connecting to analytics database")
    
    with analytics_engine.begin() as conn:
        conn.execute(text(extract_insert_daily_sales_sql))

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()