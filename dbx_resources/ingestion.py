# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is used to read cost exported data and store it in a table

# COMMAND ----------
dbutils.widgets.text("source", "/Volumes/cost_analysis/azure/export/test", "UC Volume Source Location")
dbutils.widgets.text("checkpoint", "/Volumes/cost_analysis/azure/export/checkpoint", "UC Volume Checkpoint Location")
dbutils.widgets.text("table_name", "cost_analysis.azure.table_name", "Destination Table Name")

source = dbutils.widgets.get("source")
checkpoint = dbutils.widgets.get("checkpoint")
destinaiton_table_name = dbutils.widgets.get("table_name")

# COMMAND ----------
# Set the auto loader options
autoLoaderOptions = {
  "cloudFiles.format": "csv",
  "cloudFiles.schemaLocation": checkpoint,
  "cloudFiles.schemaEvolutionMode": "addNewColumns", # Default value 
  "header": "true",
  "sep": ","
}

df = (
    spark.readStream
     .format("cloudFiles")
     .options(**autoLoaderOptions)
     .load(source)
     .writeStream
     .option("checkpointLocation", checkpoint)
     .trigger(availableNow=True)
     .table(destinaiton_table_name)
)