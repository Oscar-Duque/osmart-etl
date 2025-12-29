import json
from extract import extract_sicar
from db.db_helpers import get_max_id_sicar
from sqlalchemy import create_engine, text

CONFIG = json.load(open("../config.json"))

# Create connection to the cleaned data database (osmart_data)
db_config = CONFIG["analytics_db"]
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

payment_issues_file = "data/payment_issues.csv"

qa_header_needed = True
dropped_header_needed = True

source = CONFIG["sicar_sources"][1]

print(f"🚀 Extracting historical data for {source['name']}")

batch_dates = [
    ("2025-09-01", "2025-09-30"),
    ("2025-10-01", "2025-10-31")
]
    
for df in extract_sicar(source, batch_dates):
    df.to_sql(
        "ventas_limpias", 
        con=engine, 
        if_exists="append", 
        index=False
    )
    
# actualizar tabla de etl_progress
max_ven_id = get_max_id_sicar(engine, source['name'])

with engine.begin() as conn:
    conn.execute(
        text("""
            UPDATE etl_progress
            SET last_processed_ven_id = :last_id
            WHERE store_name = :store
        """),
        {"store": source['name'], "last_id": max_ven_id}
    )

print(f"✅ Clean data written to ventas_limpias for {source['name']}")