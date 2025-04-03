import json
from extract import extract_legacy
from transform import clean_and_standardize_legacy
import pandas as pd

CONFIG = json.load(open("config.json"))
all_clean_data = []
all_qa_data = []
all_droped_data = []

for source in CONFIG["mybusiness_sources"]:
    print(f"🚀 Extracting historical data for {source['name']}")
    df = extract_legacy(source)
    
    if not df.empty:
        df_dict = clean_and_standardize_legacy(df, source["store"])
        all_clean_data.append(df_dict["clean"])
        all_qa_data.append(df_dict["qa"])
        all_droped_data.append(df_dict["dropped"])
    else:
        print(f"⚠️ No data extracted for {source['name']}")
    
# Merge all extracted data if available
if all_clean_data:
    merged_df = pd.concat(all_clean_data, ignore_index=True)
    merged_df.to_csv("output/sales.csv", index=False)
    print(f"✅ Historical data seeded: {len(merged_df)} rows saved to sales.csv")
else:
    print("⚠️ No data to save. CSV file was not created.")
    
if all_qa_data:
    merged_df = pd.concat(all_qa_data, ignore_index=True)
    merged_df.to_csv("data/payment_issues.csv", index=False)
    print(f"⚠️ Historical data with issues: {len(merged_df)} payment_issues.csv")
else:
    print("✅ No data with issues to save. payment_issues.csv file was not created.")
    
if all_droped_data:
    merged_df = pd.concat(all_droped_data, ignore_index=True)
    merged_df.to_csv("data/dropped_sales.csv", index=False)
    print(f"⚠️ Some sales were droped from results : {len(merged_df)} dropped_sales.csv")
else:
    print("✅ No data with issues were droped. dropped_sales.csv file was not created.")