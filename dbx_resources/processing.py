# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is used to process cost exported data 

# COMMAND ----------
import pyspark.sql.functions as F
import pyspark.sql.window as W

# COMMAND ----------
INST_POLL_TAG_NAME = "instance poll internal id" # minor typo here
INST_POLL_TAG_VALUE = "jobs_133"

# JOB_TAG_NAME = "jobs id"
# JOB_TAG_VALUE = "table_sync"

dbutils.widgets.text("source_table", "cost_analysis.azure.table_name", "Cost Exported Source Data Table")
dbutils.widgets.text("subscription_id", "19a05930-7361-4fba-9f7b-85cf2569e08d", "Azure Subscription ID")
dbutils.widgets.text("inst_pool_tag_name", INST_POLL_TAG_NAME, "Instance Pool Tag Name")
dbutils.widgets.text("inst_pool_tag_value", INST_POLL_TAG_VALUE, "Instance Pool Tag Value")
# dbutils.widgets.text("rg", "", "Azure Resouce Group")

source_table = dbutils.widgets.get("source_table")
subscription_id = dbutils.widgets.get("subscription_id")
inst_pool_tag_name = dbutils.widgets.get("inst_pool_tag_name")
inst_pool_tag_value = dbutils.widgets.get("inst_pool_tag_value")

# COMMAND ----------
df = spark.table(source_table).filter("date = '01/31/2024'")

df.display()

# COMMAND ----------
df_exp = (
    df
    .filter("date = '01/31/2024'")
    .withColumn("expanded_tags", F.from_json(F.col("tags"), "MAP<STRING,STRING>"))
    .select(F.col("*"), F.col(f"expanded_tags.{inst_pool_tag_name}").alias("inst_pool"))
    .filter(f"inst_pool = '{inst_pool_tag_value}'")
)

df_exp.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregating the inst pool cost data - it should match the cost in the cost analysis report
# MAGIC ![chat](https://raw.githubusercontent.com/wsilveira-usp/azure_dbx_cost_analysis/main/image/costanalysis_charts_instance_pool_cost.png)

# COMMAND ----------
df_agg_inst_pool = (
    df_exp.groupBy("date", "inst_pool", "meterCategory").agg(F.sum(F.col("costInBillingCurrency")).alias("cost"))
)

df_agg_inst_pool.display()

# COMMAND ----------
# Instance Pool total cost
inst_pool_total_cost = df_agg_inst_pool.groupBy("inst_pool").agg(F.sum(F.col("cost")).alias("total_cost")).collect()[0][1]

print(inst_pool_total_cost)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Filtering the job cost data and finding the job inst pool cost ratio

# COMMAND ----------
df_job = (
    df_exp.filter("meterCategory = 'Azure Databricks'")
    .withColumn("totalCostInBillingCurrency", F.sum("costInBillingCurrency").over(W.Window.partitionBy()))
    .withColumn("job_inst_pool_cost", F.lit(inst_pool_total_cost) * (F.col("costInBillingCurrency") / F.col("totalCostInBillingCurrency")))
)

df_job.display()

# COMMAND ----------
df_job.select("billingAccountId", "date", "ProductName", "costInBillingCurrency", "job_inst_pool_cost", "tags").display()
