from prefect import flow, task, get_run_logger
from src.sources.dlt_sources import read_gsheet
from googleapiclient.discovery import build
from google.oauth2 import service_account
import logging
import dlt
import pandas as pd
import redis
import os

# Environment variables
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
REDIS_HOST = os.getenv("REDIS_HOST")
DATA_BASE_URL = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}'

# Google credentials
creds = service_account.Credentials.from_service_account_file(
    'credentials.json',
    scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
)

# Redis connection
r = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
try:
    r.ping()
    print("Connected to Redis")
except redis.ConnectionError:
    print("Failed to connect to Redis")

@task(name="load-updates", retries=3, retry_delay_seconds=10)
def load_sheets(spreadsheetId: str, named_range: str,credentials: service_account.Credentials, logger: logging.Logger):
    pipeline = dlt.pipeline(pipeline_name="load-google-sheets", destination=dlt.destinations.postgres(DATA_BASE_URL)).run(
        read_gsheet(spreadsheetId, named_range,credentials, logger), table_name='dataset')
    logger.info(f"Pipeline result: {pipeline}")

@flow(description="Google-Sheets-Extract-Load-Transform-Flow")
def google_sheets_pipeline():
    logger = get_run_logger()
    if r.exists('gsheets_queue') == 0:
        logger.info('There is no queue created with updates yet')
    else: 
        while True:
            task = r.brpoplpush('gsheets_queue', 'gsheets_processing', timeout=10)
            if task:
                try:
                    named_range = 'data'
                    logger.info(f"Started loading file {task}")
                    load_sheets(task, named_range,creds, logger)
                    logger.info(f"File load completed successfully for {task}")
                    r.lrem('gsheets_processing', 1, task)  # Remove from processing queue
                except Exception as e:
                    logger.error(f"Loading failed: {task}, error: {str(e)}")
                    r.lpush('gsheets_failed', task)  # Push to failed queue
                    r.lrem('gsheets_processing', 1, task)
            else:
                logger.info("Queue is empty. Waiting for tasks...")
                break

if __name__ == "__main__":
    import time
    s = time.perf_counter()
    google_sheets_pipeline.serve(name="file-loading-deployment")
    elapsed = time.perf_counter() - s
    print(f"Data loading pipeline run in {elapsed:0.2f} seconds.")