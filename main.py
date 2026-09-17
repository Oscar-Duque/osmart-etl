from pathlib import Path
import logging

from etl_sales.update_clean_data import main as update_sales
from etl_inventory.update_raw_stock_movements import main as update_raw_stock_movements
from etl_inventory.update_stock_points_v2 import main as update_stock_points

SCRITP_DIR = Path(__file__).resolve().parent
LOG_PATH = SCRITP_DIR / "logs/etls.log"

log_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

file_handler = logging.FileHandler(LOG_PATH)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_formatter)


logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

def main():
    logging.info("🚀 Starting ETL pipeline")
    
    try:
        update_sales()
    except Exception as e:
        logging.exception("❌ Sales ETL failed")
        
    try:
        update_raw_stock_movements()
    except Exception as e:
        logging.exception("❌ Raw stock movements ETL failed")
    
    try:
        update_stock_points()
    except Exception as e:
        logging.exception("❌ Stock points ETL failed")
        
if __name__ == "__main__":
    main()