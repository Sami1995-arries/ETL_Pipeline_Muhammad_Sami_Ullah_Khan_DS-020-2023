!pip install pymongo
import pandas as pd
import requests
import json
import gspread
from pymongo import MongoClient
from datetime import datetime

# Authenticate using credentials.json
gc = gspread.service_account(filename="config\db_config.json")

# Open Google Sheet using its URL
sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1oW4E5NPQTM0DXf1Clp3JiT5ljVfCWfSOxVM7DJn7Iyk/edit?gid=0#gid=0")

# Select the first sheet
ws = sh.sheet1  

# Convert sheet data into a Pandas DataFrame
google_data = pd.DataFrame(ws.get_all_records())

# Extract from CSV
csv_data = pd.read_csv("data/sample_disease_data.csv")

# Extract from JSON
with open("data/sample_data.json") as f:
    json_data = pd.DataFrame(json.load(f))

# Extract from NoSQL (MongoDB)
client = MongoClient("mongodb+srv://aabdulsami426:Sam_5073hotmail@cluster0.xwf8e.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["covid_data"]
mongo_data = pd.DataFrame(list(db["daily_reports"].find()))


# Standardize column names
def standardize_columns(df):
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    return df

google_data = standardize_columns(google_data)
csv_data = standardize_columns(csv_data)
json_data = standardize_columns(json_data)
mongo_data = standardize_columns(mongo_data)

# Data Cleaning
def Neat_clean_data(df):
    df.ffill(inplace=True)  # Forward fill missing values
    df.drop_duplicates(inplace=True)  # Remove duplicates
    return df

google_data = Neat_clean_data(google_data)
csv_data = Neat_clean_data(csv_data)
json_data = Neat_clean_data(json_data)
mongo_data = Neat_clean_data(mongo_data)


for df in [google_data, csv_data, json_data,mongo_data]:
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# Data Validation: Removing invalid records (e.g., negative values in numerical fields)
numeric_cols = ["new_confirmed", "new_deceased", "new_recovered", "new_tested", 
                "cumulative_confirmed", "cumulative_deceased", "cumulative_recovered", "cumulative_tested"]


def validate_data(df):
    for col in numeric_cols:
        if col in df.columns:
            df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')  # Use .loc to avoid warning
            df = df[df[col] >= 0].copy()  # Use .copy() to ensure it's a new DataFrame
    return df


google_data = validate_data(google_data)
csv_data = validate_data(csv_data)
json_data = validate_data(json_data)
mongo_data = validate_data(mongo_data)
# sql_data = validate_data(sql_data)
# api_data = validate_data(api_data)

# Data Aggregation: Summarizing by date and location
def aggregate_data(df):
    if "date" in df.columns and "location_key" in df.columns:
        return df.groupby(["date", "location_key"]).agg({
            "new_confirmed": "sum",
            "new_deceased": "sum",
            "new_recovered": "sum",
            "new_tested": "sum",
            "cumulative_confirmed": "max",
            "cumulative_deceased": "max",
            "cumulative_recovered": "max",
            "cumulative_tested": "max"
        }).reset_index()
    return df

google_data = aggregate_data(google_data)
csv_data = aggregate_data(csv_data)
json_data = aggregate_data(json_data)
mongo_data = aggregate_data(mongo_data)


# Filter out empty or all-NA dataframes before concatenation
data_sources = [google_data, csv_data, json_data, mongo_data]

# Remove completely empty dataframes
filtered_data_sources = [df.dropna(how="all") for df in data_sources if not df.empty]

# Concatenate only non-empty datasets
if filtered_data_sources:
    final_data = pd.concat(filtered_data_sources, ignore_index=True, sort=False)
else:
    final_data = pd.DataFrame(columns=["date", "location_key", "new_confirmed", "new_deceased", "new_recovered", 
                                       "new_tested", "cumulative_confirmed", "cumulative_deceased", 
                                       "cumulative_recovered", "cumulative_tested"])  # Ensure an empty but structured DataFrame

print("ETL pipeline complete. Processed data shape:", final_data.shape)

# Consolidate Data: Merging all datasets
#final_data = pd.concat([google_data, csv_data, json_data], ignore_index=True, sort=False)

# Load into a database (example: PostgreSQL)
# final_data.to_sql("processed_data", conn, if_exists="replace", index=False)


# Save to CSV for further analysis

final_data.to_csv("final_cleaned_data.csv", index=False)


print("ETL pipeline succeeded. Cleaned data saved as final_cleaned_data.csv")