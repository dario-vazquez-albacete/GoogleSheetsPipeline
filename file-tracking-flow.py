from prefect import flow, task, get_run_logger
from src.sources.dlt_sources import get_fileIds
from google.oauth2 import service_account
import logging
import psycopg2
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

db_connection = psycopg2.connect(
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port="5432"
)

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

@task(name="query-tracked-files", retries=3, retry_delay_seconds=10)
def get_tracked_files(logger: logging.Logger) -> pd.DataFrame:
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM information_schema.tables WHERE table_schema='file_tracking_pipeline_dataset'")
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    cursor.close()
    if len(df.index) == 0:
        tracked_files = pd.DataFrame()
        logger.info('There is no table created for file tracking')
    else:
        # Fetch tracked files from the database
        cursor = db_connection.cursor()
        cursor.execute("SELECT * FROM file_tracking_pipeline_dataset.dataset")
        result = cursor.fetchall()
        tracked_files = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
        cursor.close()
    return tracked_files

@task(name="fetch-file-updates", retries=3, retry_delay_seconds=10)
def updated_files(sheet_name: str, logger: logging.Logger)-> pd.DataFrame:
    # Make API call to check new updates
    data = get_fileIds(sheet_name, creds, logger)
    updated_files = pd.DataFrame(data)
    return updated_files

@task(name="queue-updated-files", retries=3, retry_delay_seconds=10)
def queue_updates(tracked_files_df: pd.DataFrame, updated_files_df: pd.DataFrame, logger: logging.Logger)-> pd.DataFrame:
    # Merge incoming metadata with tracking info
    merged_df = updated_files_df.merge(
    tracked_files_df, 
    how="left", 
    on="id", 
    suffixes=("", "_tracked")
    )
    # Identify new or updated files
    to_process = merged_df[
    (merged_df['modified_time'].isnull()) | 
    (merged_df['modified_time'] > merged_df['modified_time_tracked'])
    ]
    file_ids = to_process['id'].to_list()
    # Push each file ID to the Redis queue
    if len(file_ids) == 0:
        logger.info('No files to update')
        return False
    else:
        for file_id in file_ids:
            r.rpush('gsheets_queue', file_id)
            logger.info(f'File {file_id} sent to gsheets_queue')
        return True

@task(name="run-file-tracking-pipeline", retries=3, retry_delay_seconds=10)
def update_metadata(sheet_name: str, logger: logging.Logger):
    pipeline = dlt.pipeline(pipeline_name="file-tracking-pipeline", destination=dlt.destinations.postgres(DATA_BASE_URL))
    pipeline.run(get_fileIds(sheet_name, creds, logger), table_name='dataset')


@flow(name="google-drive-file-tracking-flow")
def file_tracking():
    logger = get_run_logger()
    sheet_name ='DataIngestionTest'
    current_files = get_tracked_files(logger)
    if current_files.empty:
        update_metadata(sheet_name, logger)
    else: 
        updates = updated_files(sheet_name, logger)
        if queue_updates(current_files, updates, logger):
            update_metadata(sheet_name, logger)
        else:
            pass


if __name__ == "__main__":
    import time
    s = time.perf_counter()
    file_tracking.serve(name="file-tracking-deployment")
    elapsed = time.perf_counter() - s
    print(f"File tracking pipeline run in {elapsed:0.2f} seconds.")