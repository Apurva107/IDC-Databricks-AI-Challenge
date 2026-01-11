📅 DAY 3 (11/01/26) – PySpark Transformations Deep Dive
📌 Introduction

Day 3 focused on mastering PySpark transformations used in real-world data engineering workflows. The emphasis was on understanding how PySpark differs from Pandas, performing complex joins, applying window functions for analytical use cases, and creating derived features on large-scale e-commerce data.

📚 What I Learned

Differences between PySpark and Pandas in terms of scalability and performance

How to use inner, left, right, and outer joins in PySpark

Applying window functions for running totals and ranking

Creating derived features using built-in functions and UDFs

Importance of avoiding unnecessary UDFs for performance optimization

🛠️ Tasks Performed (with Code)
1️⃣ Load Full E-Commerce Dataset
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day3").getOrCreate()

customers = spark.read.parquet("data/customers")
orders = spark.read.parquet("data/orders")
order_items = spark.read.parquet("data/order_items")

2️⃣ Perform Complex Joins
from pyspark.sql.functions import col

joined_df = (
    orders
    .join(customers, "customer_id", "inner")
    .join(order_items, "order_id", "left")
)

joined_df.show(5)

3️⃣ Window Functions (Running Totals & Ranking)
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, rank

window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy("order_date")
)

joined_df = joined_df.withColumn(
    "running_total",
    sum("order_amount").over(window_spec)
).withColumn(
    "order_rank",
    rank().over(window_spec)
)

4️⃣ Create Derived Features
from pyspark.sql.functions import when

joined_df = joined_df.withColumn(
    "spending_category",
    when(col("order_amount") < 100, "Low")
    .when(col("order_amount") < 500, "Medium")
    .otherwise("High")
)

🧠 Key Takeaways

PySpark is essential for handling large datasets that exceed single-machine limits

Window functions simplify complex analytical queries

Built-in Spark functions outperform UDFs and should be preferred

Clean joins and feature engineering are foundational for scalable data pipelines

🏷️ Tags

#PySpark #DataEngineering #BigData #SparkSQL #WindowFunctions #ETL #LearningInPublic

📖 Learning Resources

PySpark Window Functions
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html

PySpark SQL Functions
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

🙏 Acknowledgement

Thanks to the Apache Spark documentation and the open-source community for providing comprehensive resources that make learning distributed data processing accessible.
