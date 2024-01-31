# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is used to process cost exported data 

# COMMAND ----------
dbutils.widgets.text("source_table", "cost_analysis.azure.table_name", "Cost Exported Source Data Table")

source_table = dbutils.widgets.get("source_table")

# COMMAND ----------
spark.table(source_table).limit(10).display()