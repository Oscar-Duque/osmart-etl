import json
from pathlib import Path
import logging
from sqlalchemy import create_engine, text
from datetime import date, timedelta

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SOURCES = json.load(open(PROJECT_ROOT / "sources.json", encoding="utf-8"))
LOG_PATH = PROJECT_ROOT / "logs/update_daily_sales.log"

delete_recent_daily_sales_sql = (SCRIPT_DIR / "db/delete_recent_daily_sales.sql").read_text(encoding="utf-8")
extract_insert_recent_daily_sales_sql = (SCRIPT_DIR / "db/extract_insert_recent_daily_sales.sql").read_text(encoding="utf-8")
fecha_inicio = date.today() - timedelta(days=6)

def main():
    # Connect to analytics database

    logging.info("Connecting to analytics database")

    source = SOURCES["cedis_analytics"]
    analytics_engine = create_engine(
        f"mysql+pymysql://{source['user']}:"
        f"{source['password']}@{source['host']}:"
        f"{source['port']}/{source['database']}"
    )

    # Delete last 7 days

    logging.info("Deleting last 7 days from fact_ventas_diarias")

    with analytics_engine.begin() as conn:
        result = conn.execute(text(delete_recent_daily_sales_sql), {"fecha_inicio": fecha_inicio},)

    logging.info(f"Deleted {result.rowcount} rows from fact_ventas_diarias")

    # Rebuild last 7 days

    logging.info("Rebuilding last 7 days of fact_ventas_diarias")

    with analytics_engine.begin() as conn:
        result = conn.execute(text(extract_insert_recent_daily_sales_sql), {"fecha_inicio": fecha_inicio},)

    logging.info(f"Inserted {result.rowcount} rows into fact_ventas_diarias")

    logging.info("fact_ventas_diarias update completed")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler(),
        ],
    )

    main()