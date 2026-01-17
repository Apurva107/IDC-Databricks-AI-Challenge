# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH daily_revenue AS (
# MAGIC   SELECT
# MAGIC     DATE(dateTime) AS event_date,
# MAGIC     SUM(totalPrice) AS daily_revenue
# MAGIC   FROM samples.bakehouse.sales_transactions
# MAGIC   GROUP BY (dateTime)
# MAGIC )
# MAGIC SELECT
# MAGIC   event_date,
# MAGIC   daily_revenue,
# MAGIC   ROUND(AVG(daily_revenue) OVER (
# MAGIC     ORDER BY event_date
# MAGIC     ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC   ),2)AS avg_moving_7d
# MAGIC FROM daily_revenue
# MAGIC ORDER BY event_date;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   SUM(revenue) AS total_revenue,
# MAGIC   ROUND(
# MAGIC     SUM(revenue) * 100.0 / SUM(SUM(revenue)) OVER (),
# MAGIC     2
# MAGIC   ) AS revenue_contribution_pct
# MAGIC FROM ecommerce.gold.top_products
# MAGIC GROUP BY product_id
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   SUM(revenue) AS total_revenue
# MAGIC FROM ecommerce.gold.top_products
# MAGIC GROUP BY product_id
# MAGIC ORDER BY total_revenue DESC;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC WITH user_metrics AS(
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     COUNT(*) AS p_count,
# MAGIC     SUM(price) AS total_spent
# MAGIC   FROM ecommerce.silver.silver_events
# MAGIC   WHERE event_type = 'purchase'
# MAGIC   GROUP BY user_id
# MAGIC )
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN total_spent >= 5000 THEN 'VIP'
# MAGIC     WHEN total_spent >= 1000 THEN 'Loyal'
# MAGIC     ELSE 'Regular'
# MAGIC   END AS tier,
# MAGIC   COUNT(user_id) AS customers,
# MAGIC   ROUND(AVG(total_spent), 2) AS avg_ltv
# MAGIC FROM user_metrics
# MAGIC GROUP BY 1
# MAGIC ORDER BY avg_ltv DESC;