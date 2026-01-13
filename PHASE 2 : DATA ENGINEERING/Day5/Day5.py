# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

#Load the data
deltaTable = DeltaTable.forName(spark, "nov_events_table")

#incremental data
new_data = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv", header=True, inferSchema=True).limit(100)

updates = new_data.withColumn("product_name",F.coalesce(F.element_at(F.split(F.col("category_code"),r"\."), -1),F.lit("Others")))

#Implement incremental MERGE
deltaTable.alias("t").merge(
    updates.alias("s"),
    "t.user_session = s.user_session AND t.event_time = s.event_time"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("Incremental merge completed")



# COMMAND ----------

#Check the merge operation
history_df = deltaTable.history().select("version","timestamp","operation","operationMetrics")
display(history_df.limit(5))

# COMMAND ----------

#Time travel
display(deltaTable.history())
v0 = spark.read.format("delta").option("versionAsOf", 0).table("nov_events_table")
yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-01").table("nov_events_table")

# COMMAND ----------

# MAGIC  %sql
# MAGIC  OPTIMIZE nov_events_table ZORDER BY (event_type,user_id)

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC VACUUM nov_events_table RETAIN 168 HOURS;