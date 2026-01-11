# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

events = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv", header=True, inferSchema=True)
print(f"Total events: {events.count():,}")
events.printSchema()
events.show(5, truncate = False)


# COMMAND ----------

# Top 5 products by revenue
revenue = events.filter(F.col("event_type") == "purchase") \
    .groupBy("product_id", "product_id") \
    .agg(F.sum("price").alias("revenue")) \
    .orderBy(F.desc("revenue")).limit(5)
print("===Top 5 products by revenue===")
revenue.show()

# COMMAND ----------

# Running total of events per user
window = Window.partitionBy("user_id").orderBy("event_time")
events_with_cum = events.withColumn("cumulative_events", F.count("*").over(window))

print("=== Sample Running total per user ===")
events_with_cum.select("user_id","event_time", "event_type","cumulative_events").show(10)

# COMMAND ----------

# Conversion rate by category
category_conversion=events.groupBy("category_code", "event_type").count() \
    .groupBy("category_code") \
    .pivot("event_type").sum("count") \
    .withColumn("conversion_rate", F.col("purchase")/F.col("view")*100)

print("=== conversion rate by category=== ")
category_conversion.show(5)
