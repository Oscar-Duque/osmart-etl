import json
import pandas as pd
from extract import extract_legacy, extract_sicar
from transform import clean_and_standardize_legacy
from db.db_helpers import reset_ventas_limpias, insert_on_conflict_update
from sqlalchemy import create_engine
import os

CONFIG = json.load(open("../config.json"))

# Create connection to the cleaned data database (osmart_data)
db_config = CONFIG["analytics_db"]
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)
reset_ventas_limpias(engine)

# Reset CSV file for payment issues
payment_issues_file = "data/payment_issues.csv"
if os.path.exists(payment_issues_file):
    os.remove(payment_issues_file)

qa_header_needed = True
dropped_header_needed = True

for source in CONFIG["mybusiness_sources"]:
    print(f"🚀 Extracting historical data for {source['name']}")

    for df in extract_legacy(source):
        df_dict = clean_and_standardize_legacy(df, source["store"])

        df_dict["clean"].to_sql(
            "ventas_limpias", 
            con=engine, 
            if_exists="append", 
            index=False, 
            method=insert_on_conflict_update
        )
        
        # Append QA data to CSV
        if not df_dict["qa"].empty:
            df_dict["qa"].to_csv(
                "data/payment_issues.csv", 
                index=False, 
                mode='a',
                header=qa_header_needed
            )
            qa_header_needed = False

    print(f"✅ Clean data written to ventas_limpias for {source['name']}")

for source in CONFIG["sicar_sources"]:
    print(f"🚀 Extracting historical data for {source['name']}")
    batch_dates = [
        ("2024-10-27", "2024-10-31"),
        ("2024-11-01", "2024-11-30"),
        ("2024-12-01", "2024-12-31"),
        ("2025-01-01", "2025-01-31"),
        ("2025-02-01", "2025-02-28"),
        ("2025-03-01", "2025-03-31"),
        ("2025-04-01", "2025-04-30"),
        ("2025-05-01", "2025-05-31"),
    ]
        
    for df in extract_sicar(source, batch_dates):
        # clean_and_standardize_sicar(df, source["store"]) needed here?

        df.to_sql(
            "ventas_limpias", 
            con=engine, 
            if_exists="append", 
            index=False
        )
    
    print(f"✅ Clean data written to ventas_limpias for {source['name']}")