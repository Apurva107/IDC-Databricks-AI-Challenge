# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create structure
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce;
# MAGIC USE CATALOG ecommerce;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bronze_events
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM csv.`/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv`;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze_events;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze_events
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM csv.`/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv`
# MAGIC WITH(header="true", inferSchema="true");
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_events
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM bronze_events
# MAGIC WHERE event_type IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register tables
# MAGIC CREATE OR REPLACE TABLE gold_product AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   COUNT(*) AS purchases,
# MAGIC   SUM(price) AS revenue
# MAGIC FROM silver_events
# MAGIC WHERE event_type = 'purchase'
# MAGIC GROUP BY product_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Permissions
# MAGIC GRANT SELECT ON TABLE gold_product TO `apurvapatil71@gmail.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA silver TO `apurvapatil71@gmail.com`;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Controlled view
# MAGIC CREATE VIEW gold.top_products AS
# MAGIC SELECT product_id, revenue
# MAGIC FROM gold_product
# MAGIC WHERE purchases > 10
# MAGIC ORDER BY revenue DESC LIMIT 100;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.top_products

# COMMAND ----------

