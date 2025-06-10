import json
import pandas as pd
from extract import extract_legacy
from transform import clean_and_standardize_legacy
from db.db_helpers import reset_ventas_limpias, insert_on_conflict_update
from sqlalchemy import create_engine
import os

CONFIG = json.load(open("config.json"))
# all_clean_data = []
# all_qa_data = []
# all_droped_data = []

# Create connection to the cleaned data database (osmart_data)
db_config = CONFIG["clean_data_db"]
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)
reset_ventas_limpias(engine)

# Reset CSV files before starting (delete old files)
for filename in ["data/payment_issues.csv", "data/dropped_sales.csv"]:
    if os.path.exists(filename):
        os.remove(filename)

qa_header_needed = True
dropped_header_needed = True

for source in CONFIG["mybusiness_sources"]:
    print(f"🚀 Extracting historical data for {source['name']}")
    # df = extract_legacy(source)
    for df in extract_legacy(source):
    
    # if not df.empty:
        df_dict = clean_and_standardize_legacy(df, source["store"])
        # all_clean_data.append(df_dict["clean"])
        # all_qa_data.append(df_dict["qa"])
        # all_droped_data.append(df_dict["dropped"])
        
        # Write clean data chunk immediately to DB
        # df_dict["clean"].to_sql("ventas_limpias", 
        #                         con=engine, 
        #                         if_exists="append", 
        #                         index=False, 
        #                         method="multi",
        #                         chunksize=1000)
        
        # insert_chunk_with_upsert(df_dict["clean"], engine, "ventas_limpias")
        
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
                header=["venta", "fecha", "usuhora", "caja", "usuario", "total", "tarjeta_in", "efectivo_in", "otros_in", "cobranza_aplicada", "egresos", "tienda", "source_db", "source_system", "extracted_at"] if qa_header_needed else False
            )
            qa_header_needed = False
        
        # Append dropped data to CSV
        if not df_dict["dropped"].empty:
            df_dict["dropped"].to_csv("data/dropped_sales.csv", 
                index=False, 
                mode='a',
                header=["venta", "fecha", "usuhora", "caja", "usuario", "total", "tarjeta_in", "efectivo_in", "otros_in", "cobranza_aplicada", "egresos", "tienda", "source_db", "source_system", "extracted_at"] if dropped_header_needed else False
            )
            dropped_header_needed = False

    print(f"✅ Clean data written to ventas_limpias for {source['name']}")
    # else:
    #     print(f"⚠️ No data extracted for {source['name']}")
    
# Merge all extracted data if available
# if all_clean_data:
#     merged_df = pd.concat(all_clean_data, ignore_index=True)
#     # merged_df.to_csv("output/sales.csv", index=False)
#     # reset_ventas_limpias(engine)
#     # merged_df.to_sql("ventas_limpias", con=engine, if_exists="append", index=False)
#     print(f"✅ Historical data seeded: {len(merged_df)} rows saved to sales.csv")
# else:
#     print("⚠️ No data to save. CSV file was not created.")
    
# if all_qa_data:
#     merged_df = pd.concat(all_qa_data, ignore_index=True)
#     merged_df.to_csv("data/payment_issues.csv", index=False)
#     print(f"⚠️ Historical data with issues: {len(merged_df)} payment_issues.csv")
# else:
#     print("✅ No data with issues to save. payment_issues.csv file was not created.")
    
# if all_droped_data:
#     merged_df = pd.concat(all_droped_data, ignore_index=True)
#     merged_df.to_csv("data/dropped_sales.csv", index=False)
#     print(f"⚠️ Some sales were droped from results : {len(merged_df)} dropped_sales.csv")
# else:
#     print("✅ No data with issues were droped. dropped_sales.csv file was not created.")