# Databricks notebook source
# MAGIC %md
# MAGIC ### Create silver tables
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

def import_green_trip_data():
    """ Import green trip data from bronze tables and create silver tables.
    """
    print("Starting green trip data")
    # Read bronze tables
    green_tripdata_bronze_df = spark.read.table("workspace.02_bronze.green_tripdata")

    # Perform transformations to create silver tables
    silver_df = green_tripdata_bronze_df.select("VendorID", 
                                "passenger_count", 
                                "total_amount",
                                "lpep_pickup_date",
                                "lpep_pickup_datetime",
                                "lpep_dropoff_datetime")
    # Rename columns to match silver table schema
    silver_df = silver_df.withColumnRenamed("VendorID", "vendor_id")
    silver_df = silver_df.withColumnRenamed("lpep_pickup_date", "pickup_date")
    silver_df = silver_df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    silver_df = silver_df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

    # Add additional columns for silver table
    silver_df = silver_df.withColumn('color', lit("green"))
    silver_df = silver_df.withColumn('__timestamp', current_timestamp())

    # Write to silver tables
    silver_df.write.partitionBy("pickup_date").mode("overwrite").saveAsTable("workspace.03_silver.new_york_taxi")

    print("Finishing green trip data")

def import_yellow_trip_data():
    """ Import yellow trip data from bronze tables and create silver tables.
    """
    print("Starting yellow trip data")

    # Read bronze tables
    yellow_tripdata_bronze_df = spark.read.table("workspace.02_bronze.yellow_tripdata")

    silver_df = yellow_tripdata_bronze_df.select("VendorID", 
                                "passenger_count", 
                                "total_amount",
                                "tpep_pickup_date",
                                "tpep_pickup_datetime",
                                "tpep_dropoff_datetime")

    # Rename columns to match silver table schema
    silver_df = silver_df.withColumnRenamed("VendorID", "vendor_id")
    silver_df = silver_df.withColumnRenamed("tpep_pickup_date", "pickup_date")
    silver_df = silver_df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    silver_df = silver_df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

    # Add additional columns for silver table
    silver_df = silver_df.withColumn('color', lit("yellow"))
    silver_df = silver_df.withColumn('__timestamp', current_timestamp())

    silver_df.write.mode("append").saveAsTable("workspace.03_silver.new_york_taxi")

    print("Finishing yellow trip data")

def run():
    """ Run the import process for green and yellow trip data.
    """
    import_green_trip_data()
    import_yellow_trip_data()

run()

