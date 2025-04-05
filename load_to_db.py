from pymongo import MongoClient
import json

# Correct way to initialize the MongoClient
client = MongoClient("mongodb+srv://aabdulsami426:Sam_5073hotmail@cluster0.xwf8e.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

# Access the database and collection
db = client["covid_data"]
collection = db["daily_reports"]

# Load data from the file
with open('data.json', 'r') as file:
    json_data = json.load(file)

# Display the loaded data
print(json_data)

# Insert the data into MongoDB
collection.insert_many(json_data)

print("Bulk data inserted successfully!")