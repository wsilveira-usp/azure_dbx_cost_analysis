# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is used to read cost exported data and store it in a table

# COMMAND ----------
import pyspark.sql.functions as F

# COMMAND ----------
dbutils.widgets.text("source", "/Volumes/cost_analysis/azure/export/test", "UC Volume Source Location")
dbutils.widgets.text("checkpoint", "/Volumes/cost_analysis/azure/export/checkpoint", "UC Volume Checkpoint Location")
dbutils.widgets.text("table_name", "cost_analysis.azure.table_name", "Destination Table Name")

source = dbutils.widgets.get("source")
checkpoint = dbutils.widgets.get("checkpoint")
destination_table_name = dbutils.widgets.get("table_name")

# COMMAND ----------
# Drop table and data
# spark.sql(f"DROP TABLE IF EXISTS {destination_table_name}")
# dbutils.fs.rm(checkpoint, True)

# COMMAND ----------
# Set the auto loader options
autoLoaderOptions = {
  "cloudFiles.format": "csv",
  "cloudFiles.schemaLocation": checkpoint,
  "cloudFiles.schemaEvolutionMode": "addNewColumns", # Default value 
  "header": "true",
  "sep": ",",
  "escape": "\""
}

df = (
    spark.readStream
     .format("cloudFiles")
     .options(**autoLoaderOptions)
     .load(source)
     .drop_duplicates()
     .withColumn("ingestion_time", F.current_timestamp())
     .withColumn("input_file_name", F.input_file_name())
     .writeStream
     .option("checkpointLocation", checkpoint)
     .trigger(availableNow=True)
     .table(destination_table_name)
)