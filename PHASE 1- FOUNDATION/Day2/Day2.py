# Databricks notebook source
# Load data into the dataframe. 
df = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv", header=True, inferSchema=True)



# COMMAND ----------

# Basic operation to see columns and data
df.select("event_type", "product_id", "price").show(10)


# COMMAND ----------

#Filter by price greater than 100
df.filter("price > 100").count()

# COMMAND ----------

#Count of types of events
df.groupBy("event_type").count().show()

#count of top brands
top_brands = df.groupBy("brand").count().orderBy("count", ascending=False).limit(5)
top_brands.show()