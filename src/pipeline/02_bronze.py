# Databricks notebook source
# MAGIC %md
# MAGIC ### Create bronze tables

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta

DATA_FILE_TYPES = ['green_tripdata',  'yellow_tripdata']

TABLE_CONFIGURATION = {}

TABLE_CONFIGURATION["green_tripdata"] = {
  "partition_key" : "lpep_pickup_date",
  "partition_key_datetime" : "lpep_pickup_datetime",
  "columns" :	[{"column_name":"VendorID","data_type":"LONG"},{"column_name":"lpep_pickup_datetime","data_type":"TIMESTAMP_NTZ"},{"column_name":"lpep_dropoff_datetime","data_type":"TIMESTAMP_NTZ"},{"column_name":"store_and_fwd_flag","data_type":"STRING"},{"column_name":"RatecodeID","data_type":"DOUBLE"},{"column_name":"PULocationID","data_type":"LONG"},{"column_name":"DOLocationID","data_type":"LONG"},{"column_name":"passenger_count","data_type":"DOUBLE"},{"column_name":"trip_distance","data_type":"DOUBLE"},{"column_name":"fare_amount","data_type":"DOUBLE"},{"column_name":"extra","data_type":"DOUBLE"},{"column_name":"mta_tax","data_type":"DOUBLE"},{"column_name":"tip_amount","data_type":"DOUBLE"},{"column_name":"tolls_amount","data_type":"DOUBLE"},{"column_name":"ehail_fee","data_type":"INT"},{"column_name":"improvement_surcharge","data_type":"DOUBLE"},{"column_name":"total_amount","data_type":"DOUBLE"},{"column_name":"payment_type","data_type":"DOUBLE"},{"column_name":"trip_type","data_type":"DOUBLE"},{"column_name":"congestion_surcharge","data_type":"DOUBLE"}]}

TABLE_CONFIGURATION["yellow_tripdata"] = {
  "partition_key" : "tpep_pickup_date",
  "partition_key_datetime" : "tpep_pickup_datetime",
  "columns" :	[{"column_name":"VendorID","data_type":"LONG"},{"column_name":"tpep_pickup_datetime","data_type":"TIMESTAMP_NTZ"},{"column_name":"tpep_dropoff_datetime","data_type":"TIMESTAMP_NTZ"},{"column_name":"passenger_count","data_type":"DOUBLE"},{"column_name":"trip_distance","data_type":"DOUBLE"},{"column_name":"RatecodeID","data_type":"DOUBLE"},{"column_name":"store_and_fwd_flag","data_type":"STRING"},{"column_name":"PULocationID","data_type":"LONG"},{"column_name":"DOLocationID","data_type":"LONG"},{"column_name":"payment_type","data_type":"LONG"},{"column_name":"fare_amount","data_type":"DOUBLE"},{"column_name":"extra","data_type":"DOUBLE"},{"column_name":"mta_tax","data_type":"DOUBLE"},{"column_name":"tip_amount","data_type":"DOUBLE"},{"column_name":"tolls_amount","data_type":"DOUBLE"},{"column_name":"improvement_surcharge","data_type":"DOUBLE"},{"column_name":"total_amount","data_type":"DOUBLE"},{"column_name":"congestion_surcharge","data_type":"DOUBLE"},{"column_name":"airport_fee","data_type":"DOUBLE"}]}

def get_list_bronze_tables():
    """
    Retrieve the list of bronze tables from the workspace information schema.   
    :return: A list of table names in the '02_bronze' schema.
    """
    list_table_names = spark.sql("SELECT t.table_name FROM workspace.information_schema.tables as t where t.table_schema = '02_bronze'").select("table_name").collect()

    table_names = [f"workspace.02_bronze.{table_name['table_name']}" for table_name in list_table_names]
    
    return table_names

def import_bronze(date_to_ingest, file_type: str, list_table_names: list):
    """
    Import files from the specified path and create tables in the workspace.
    
    :param date_to_ingest: The date for which the files are to be imported.
    :param file_type: The type of file to be imported (e.g., 'fhv_tripdata', 'fhvhv_tripdata', etc.).
    :param list_table_names: A list of existing table names in the '02_bronze' schema.
    :return: None
    """

    transient_table_name = f"{file_type}_{date_to_ingest.year}_{date_to_ingest.month:02}".strip()
    full_transient_table_name = f"workspace.01_transient.{transient_table_name}"
    bronze_table_name = f"workspace.02_bronze.{file_type}".strip()

    bronze_table_configuration = TABLE_CONFIGURATION[file_type]

    if bronze_table_name not in list_table_names:
        sql = get_create_table_command(bronze_table_name=bronze_table_name,
                                      bronze_table_configuration=bronze_table_configuration)

        print(f"Creating table {bronze_table_name} with command {sql}")                              
        spark.sql(sql)

    columns_list_description = get_columns_list_description(bronze_table_configuration=bronze_table_configuration)

    sql = f"DELETE FROM {bronze_table_name} WHERE transient_table_name = '{transient_table_name}'"

    spark.sql(sql)

    sql = f"""INSERT INTO {bronze_table_name} ({columns_list_description},transient_table_name) SELECT {columns_list_description}, '{transient_table_name}' FROM {full_transient_table_name}
    where (total_amount < 0
or passenger_count < 0 or trip_distance < 0 or fare_amount < 0 or extra < 0 or mta_tax < 0 or tip_amount < 0 or tolls_amount < 0 or improvement_surcharge < 0) = False;"""
        
    spark.sql(sql)

    print(f"Finishing importing {bronze_table_name} for {date_to_ingest.year}-{date_to_ingest.month:02}.")

def get_create_table_command(bronze_table_name: str, bronze_table_configuration: dict):
    """
    Generate the SQL command to create a table in the '02_bronze' schema.
    :return: The SQL command as a string.
    """
    sql = f"CREATE TABLE {bronze_table_name} ("
    
    for column in bronze_table_configuration['columns']:
        column_name = column['column_name']
        data_type = column['data_type']
        column_definition = f"{column_name} {data_type}"
        
        sql = f"{sql}{column_definition}, "

    sql = f"""{sql} {bronze_table_configuration['partition_key']} DATE GENERATED ALWAYS AS (CAST({bronze_table_configuration['partition_key_datetime']} AS DATE)),"""

    sql = f"""{sql} transient_table_name STRING,
              ingestion_datetime TIMESTAMP GENERATED ALWAYS AS (now())"""

    sql = f"""{sql} ) PARTITIONED BY ({bronze_table_configuration['partition_key']})
    TBLPROPERTIES (delta.feature.timestampNtz = 'supported', delta.autoOptimize.autoCompact = true, delta.feature.allowColumnDefaults = 'supported');"""

    return sql

def get_columns_list_description(bronze_table_configuration: dict):
    """
    Generate a comma-separated list of column names for the table.
    
    :param bronze_table_configuration: The configuration dictionary for the bronze table.
    :return: A string containing the column names separated by commas.
    """
    columns_list_description = ", ".join([f"{column['column_name']}" for column in bronze_table_configuration['columns']])
    
    return columns_list_description

def run(start_date=None, end_date=None):
    """ Run the import process for the specified date range and file types.
    """
    spark.sql("USE CATALOG `workspace`;")
    spark.sql("USE SCHEMA `02_bronze`;")

    list_table_names = get_list_bronze_tables()

    for file_type in DATA_FILE_TYPES:
        date_to_ingest = start_date
        while date_to_ingest <= end_date:
            import_bronze(date_to_ingest, file_type, list_table_names)

            if file_type not in list_table_names:
              list_table_names.append(f"workspace.02_bronze.{file_type}")

            date_to_ingest = date_to_ingest + relativedelta(months=1)

start_date = datetime(year=2023, month=1, day=1)
end_date = datetime(year=2023, month=5, day=1)
run(start_date, end_date)