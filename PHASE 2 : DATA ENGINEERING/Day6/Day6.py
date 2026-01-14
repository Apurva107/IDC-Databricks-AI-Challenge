# Databricks notebook source
# BRONZE: Raw ingestion
from pyspark.sql import functions as F
raw = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv", header=True, inferSchema=True)
raw.withColumn("ingestion_ts", F.current_timestamp()) \
   .write.format("delta").mode("overwrite").saveAsTable("bronze_events")

print(f"Bronze Table created : {spark.table('bronze_events').count()} rows.")

# COMMAND ----------

# SILVER: Cleaned data
bronze = spark.read.table("bronze_events")
silver = bronze.filter(F.col("price") > 0) \
    .filter(F.col("price") < 10000) \
    .dropDuplicates(["user_session", "event_time"]) \
    .withColumn("event_date", F.to_date("event_time")) \
    .withColumn("product_name", F.coalesce(F.element_at(F.split(F.col("category_code"),r"\."),-1), F.lit("Others")))\
    .withColumn("price_tier",
        F.when(F.col("price") < 10, "budget")
         .when(F.col("price") < 50, "mid")
         .otherwise("premium"))
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_events")
display(spark.table("silver_events").limit(5))

# COMMAND ----------

# GOLD: Aggregates
silver = spark.read.table("silver_events")
product_perf = silver.groupBy("product_id", "product_name") \
    .agg(
        F.countDistinct(F.when(F.col("event_type")=="view",F.col("user_id"))).alias("unique_views"),
        F.countDistinct(F.when(F.col("event_type")=="purchase", F.col("user_id"))).alias("unique_purchases"),
        F.sum(F.when(F.col("event_type")=="purchase", F.col("price"))).alias("total_revenue")
    ).withColumn("conversion_rate", F.round(F.try_divide(F.col("unique_purchases"), F.col("unique_views"))*100,2))
product_perf.write.format("delta").mode("overwrite").saveAsTable("gold_product")
display(spark.table("gold_product").orderBy(F.desc("total_revenue")).limit(10))
        

# COMMAND ----------

print(f"Bronze total : {spark.table('bronze_events').count()}")
print(f"Silver total : {spark.table('silver_events').count()}")
print(f"Gold total : {spark.table('gold_product').count()}")