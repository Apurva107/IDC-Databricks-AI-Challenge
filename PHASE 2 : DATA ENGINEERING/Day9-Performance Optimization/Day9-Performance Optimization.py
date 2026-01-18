# Databricks notebook source
spark.sql("""
SELECT *
FROM ecommerce.silver.silver_events
WHERE event_type = 'purchase'
""").explain(True)

# COMMAND ----------

spark.sql("DESCRIBE DETAIL ecommerce.silver.silver_events").show(truncate=False)

# COMMAND ----------

spark.sql("""
OPTIMIZE ecommerce.silver.silver_events
ZORDER BY (user_id)
""")


# COMMAND ----------

spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.silver.customer_transaction_part
        USING DELTA
        PARTITIONED BY (event_type)
        AS
        SELECT *
        FROM ecommerce.silver.silver_events
    """)


# COMMAND ----------

df = spark.sql("""
SELECT *
FROM ecommerce.silver.customer_transaction_part
WHERE event_type = 'purchase'
""")

df.show(truncate=False)


# COMMAND ----------

spark.sql("DESCRIBE DETAIL ecommerce.silver.silver_events").show(truncate=False)

# COMMAND ----------

spark.sql("""
          OPTIMIZE ecommerce.silver.customer_transaction_part
          ZORDER BY (user_id)""")

# COMMAND ----------

import time
start = time.time()
spark.sql("""
SELECT *
FROM ecommerce.silver.customer_transaction_part
WHERE event_type = 'purchase' AND user_id = 12345
""").count()
print(f"After optimize: {time.time() - start:.2f}s")
