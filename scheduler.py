!pip install schedule
import schedule
import time
import subprocess

def run_etl_pipeline():
    print("Running ETL Pipeline...")
    try:
        # Call the main ETL script
        subprocess.run(["python", "etl_pipeline.py"], check=True)
        print("ETL Pipeline Completed Successfully")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running the ETL pipeline: {e}")

# Schedule the ETL pipeline to run daily at midnight
schedule.every().day.at("00:00").do(run_etl_pipeline)

# Keep the script running to execute scheduled tasks
if __name__ == "__main__":
    print("Scheduler is running. ETL Pipeline will run every day at midnight.")
    while True:
        schedule.run_pending()
        time.sleep(1)
