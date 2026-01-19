# Databricks notebook source
events = spark.table("ecommerce.silver.silver_events")
events.describe(["price"]).show()


# COMMAND ----------

from pyspark.sql import functions as F

weekday = events.withColumn(
    "is_weekend",
    F.dayofweek("event_time").isin([1, 7])
)

weekday.groupBy("is_weekend", "event_type").count().show()


# COMMAND ----------

from pyspark.sql.functions import col
corre_df = events.withColumn(
    "price_float",
    col("price").cast("float")
) 
corre_df.stat.corr("price_float", "price_float")


# COMMAND ----------

# Feature engineering
from pyspark.sql.window import Window
features = events.withColumn("hour", F.hour("event_time")) \
    .withColumn("day_of_week", F.dayofweek("event_time")) \
    .withColumn("price_log", F.log(F.col("price")+1)) \
    .withColumn("time_since_first_view",
        F.unix_timestamp("event_time") -
        F.unix_timestamp(F.first("event_time").over(Window.partitionBy("user_id").orderBy("event_time")))
    )

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

