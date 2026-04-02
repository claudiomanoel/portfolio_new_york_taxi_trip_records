# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Create gold tables
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import hour, col, sum, count, lit

def run():
    # Load the silver table
    silver_df = spark.table("workspace.03_silver.new_york_taxi")

    # Group by hour and aggregate the necessary columns
    gold_df = silver_df.withColumn("pickup_hour", hour(col("pickup_datetime"))) \
                    .groupBy("pickup_date", "pickup_hour", "color") \
                    .agg(
                            count(lit(1)).alias("quantity"),
                            sum("total_amount").alias("total_amount"),
                            sum("passenger_count").alias("passenger_count"),
                        )

    # Save the result as a gold table
    gold_df.write.mode("overwrite").saveAsTable("workspace.04_gold.new_york_taxi_by_hour")

run()