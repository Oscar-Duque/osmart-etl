import json
import pandas as pd
from extract import extract_legacy
from transform import clean_and_standardize_legacy
from db.db_helpers import reset_ventas_limpias, insert_on_conflict_update
from sqlalchemy import create_engine
import os

CONFIG = json.load(open("config.json"))

# Create connection to the cleaned data database (osmart_data)
db_config = CONFIG["clean_data_db"]
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
    
    # if not df.empty:
        df_dict = clean_and_standardize_legacy(df, source["store"])
        
        # Write clean data chunk immediately to DB
        df_dict["clean"].to_sql("ventas_limpias", 
                                con=engine, 
                                if_exists="append", 
                                index=False, 
                                method=insert_on_conflict_update,
                                chunksize=1000)
        
        # Append QA data to CSV
        if not df_dict["qa"].empty:
            df_dict["qa"].to_csv("data/payment_issues.csv", 
                index=False, 
                mode='a',
                header=qa_header_needed
            )
            qa_header_needed = False

    print(f"✅ Clean data written to ventas_limpias for {source['name']}")
