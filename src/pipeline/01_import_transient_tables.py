# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Create transient tables from file
# MAGIC

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta

DATA_FILE_TYPES = ['green_tripdata',  'yellow_tripdata']
VOLUME_PATH = '/Volumes/workspace/01_transient/new_york_trips'

def import_files(date_to_ingest, file_type: str):
    """
    Import files from the specified path and create tables in the workspace.
    
    :param date_to_ingest: The date for which the files are to be imported.
    :param file_type: The type of file to be imported (e.g., 'green_tripdata', 'yellow_tripdata', etc.).
    :return: None
    """

    file_name = f"{VOLUME_PATH}/{file_type}_{date_to_ingest.year}-{date_to_ingest.month:02}.parquet"
    table_name = f"workspace.01_transient.{file_type}_{date_to_ingest.year}_{date_to_ingest.month:02}".strip()
        
    sql = f"CREATE OR REPLACE TABLE {table_name} TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported') AS SELECT * FROM PARQUET.`{file_name}`;"
        
    spark.sql(sql)

    print(f"Finishing importing {file_name}")

def run(start_date=None, end_date=None):
    """ Run the import process for the specified date range and file types.
    """
    spark.sql("USE CATALOG `workspace`;")
    spark.sql("USE SCHEMA `01_transient`;")

    for file_types in DATA_FILE_TYPES:
        date_to_ingest = start_date
        while date_to_ingest <= end_date:
            import_files(date_to_ingest=date_to_ingest, file_type=file_types)

            date_to_ingest = date_to_ingest + relativedelta(months=1)

start_date = datetime(year=2023, month=1, day=1)
end_date = datetime(year=2023, month=5, day=1)
run(start_date, end_date)