import json
from extract import extract_sicar
from etl_sales.db_helpers import get_max_id_sicar
from sqlalchemy import create_engine, text

CONFIG = json.load(open("../config_v2.json"))

payment_issues_file = "data/payment_issues.csv"


analytics_source = CONFIG["cedis"]["analytics_source"]

analytics_engine = create_engine(
    f"mysql+pymysql://{analytics_source['user']}:{analytics_source['password']}@{analytics_source['host']}:{analytics_source['port']}/{analytics_source['database']}"
)


store_name = 'zapata'
source = CONFIG["zapata"]["sicar_source"]

print(f"🚀 Extracting historical data for {store_name}")

batch_dates = [
    ("2025-12-01", "2026-01-02")
]
    
for df in extract_sicar(source, store_name, batch_dates):
    df.to_sql(
        "ventas_limpias", 
        con=analytics_engine, 
        if_exists="append", 
        index=False
    )

# actualizar tabla de etl_progress
max_ven_id = get_max_id_sicar(analytics_engine, store_name) or 0

with analytics_engine.begin() as conn:
    conn.execute(
        text("""
            UPDATE etl_progress
            SET last_processed_ven_id = :last_id
            WHERE store_name = :store
        """),
        {"store": store_name, "last_id": max_ven_id}
    )

print(f"✅ Clean data written to ventas_limpias for {store_name}")