# ETL Pipeline Project

## Overview

This project involves building an ETL (Extract, Transform, Load) pipeline that collects data from various sources, processes it, and stores it into a database for further analysis. The project is structured to allow easy integration with various data sources, including CSV files, JSON APIs, Google Sheets, and NoSQL database. The data is cleaned, transformed, and then loaded into a database (MongoDB) for further analysis.

### Directory Structure


## Data Sources

The ETL pipeline works with the following data sources:
- **CSV File**: Example data is stored in `data/sample_disease_data.csv`.
- **JSON API**: Covid Patients data is fetched from `data/sample_data.json`.
- **Google Sheets**: Data is fetched from an exported Google Sheet available in `data/google_sheet_sample.csv`.
- **MongoDB Database**: Connects to a Mongo DB database to fetch and load data.
- **REST API**: Data fetched in real-time (such as Daliy covid cases information ).

## Data Processing

### 1. Data Cleaning
The pipeline handles missing values, duplicates, and erroneous data through the following steps:
- **Forward-fill missing values**: Fills in any missing values in the dataset.
- **Remove duplicates**: Ensures there are no repeated records.
- **Convert columns**: Standardizes column names and formats.

### 2. Unit Conversions
The pipeline can standardize units.

### 3. Timestamp Formatting
All datetime values are normalized to the ISO 8601 format to ensure consistency across datasets.

### 4. Feature Engineering
New calculated columns are created based on existing data, such as calculating a "weather impact score" from temperature, humidity, and wind speed.

## Automation

### Scheduling with Python
The ETL pipeline is scheduled to run at regular intervals (e.g., daily) using the Python `schedule` library. This ensures that data is regularly extracted, processed, and loaded into the database.

```bash
python scheduler.py
