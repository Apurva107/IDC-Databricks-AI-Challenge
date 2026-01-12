# Databricks notebook source
#Load  dataset
nov_events = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv", header=True, inferSchema=True)
nov_events.printSchema()
nov_events.show(5)

# COMMAND ----------

# Convert CSV to Delta Format
nov_events.write.format("delta").mode("overwrite").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/nov_events")

# COMMAND ----------

# Create managed table
nov_events.write.format("delta").mode("overwrite").saveAsTable("nov_events_table")


# COMMAND ----------

# SQL approach
spark.sql("""
    CREATE TABLE nov_events_delta
    USING DELTA
    AS SELECT * FROM nov_events_table
""")

# COMMAND ----------

sample = nov_events.limit(100)
sample.write.format("delta").mode("overwrite").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/nov_events")

# COMMAND ----------

from pyspark.sql import Row
try:
    wrong_schema = spark.createDataFrame([("a","b","c")], ["x","y","z"])
    wrong_schema.write.format("delta").mode("append").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/nov_events")
except Exception as e:
    print(f"Schema enforcement: {e}")