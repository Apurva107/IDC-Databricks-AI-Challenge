# Databricks notebook source
# Add widgets for parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("source_path", "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv","Source Path")
dbutils.widgets.dropdown("layer", "bronze", ["bronze","silver","gold"],"Selecting Processing layer")

source_file = dbutils.widgets.get("source_path")
layer = dbutils.widgets.get("layer")
print(f"Job started layer : {layer}")
print(f"processing source: {source_file}")


# COMMAND ----------

from pyspark.sql import functions as F
def process_bronze():
    print("Running Bronze layer.")
    raw_df = spark.read.csv(source_file, header=True)
    raw_df.withColumn("ingestion_ts", F.current_timestamp()).write.mode("overwrite").format("delta").saveAsTable("bronze_events")
    return "Bronze success"

def process_silver():
    print("Running Silver layer.")
    bronze_df = spark.read.table("bronze_events")
    silver_df = bronze_df.filter(F.col("price") > 0)\
        .dropDuplicates([ "user_session","event_time"])\
        .withColumn("product_name",F.coalesce(F.element_at(F.split(F.col("category_code"),r"\."),-1),F.lit("Other")))
    silver_df.write.mode("overwrite").format("delta").saveAsTable("silver_events")
    return "Silver success"

def process_gold():
    print("Running Gold layer.")
    silver_df = spark.read.table("silver_events")
    gold_df = silver_df.groupBy("product_id","product_name").agg(F.sum("price").alias("total_revenue"))
    gold_df.write.mode("overwrite").format("delta").saveAsTable("gold_events")
    return "Gold success"


# COMMAND ----------

#1.Create the job
#click jobs & pipelines in the sidebar -> create job.

#Task 1: Bronze
#Name: bronze_ingestion
#Type:Notebook
#parameters: click add, key: layer, value: bronze
#                       key: source_path, value: /Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv

#Task 2: Silver
#click the + icon below task 1
#Name: silver_cleaning
#depends on: bronze_ingestion
#parameters: key: layer, value: silver

#Task 3: Gold
#click the + icon below task 2
#Name: gold_aggregation
#depends on: silver_cleaning
#parameters: key: layer, value: gold

#2.set dependency(task3)
#by setting the "depends on" field, you ensure the silver won't start until the bronze is finished.
#this is calleda DAG.

#3.schedule execution(task4)
#on the right panel of the job UI, look for schedule