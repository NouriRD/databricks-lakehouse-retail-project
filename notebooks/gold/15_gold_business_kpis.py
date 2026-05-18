# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 1 — Total Revenue**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(SUM(total_revenue), 2) AS total_revenue
# MAGIC FROM fact_order_items

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 2 — Revenue by Product Category**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC     ROUND(SUM(total_revenue), 2) AS revenue
# MAGIC FROM fact_order_items
# MAGIC GROUP BY product_category
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 3 — Top Seller States**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     seller_state,
# MAGIC     ROUND(SUM(total_revenue), 2) AS revenue
# MAGIC FROM fact_order_items
# MAGIC GROUP BY seller_state
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 4 — Payment Type Analysis**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     payment_type,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     ROUND(SUM(total_revenue), 2) AS revenue
# MAGIC FROM fact_order_items
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 5 — Delivery Performance**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     is_delivered,
# MAGIC     AVG(delivery_days) AS avg_delivery_days
# MAGIC FROM fact_order_items
# MAGIC GROUP BY is_delivered

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 6 — Monthly Revenue Trend**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC
# MAGIC     ROUND(SUM(f.total_revenue), 2) AS revenue
# MAGIC
# MAGIC FROM fact_order_items f
# MAGIC
# MAGIC JOIN dim_date d
# MAGIC ON TO_DATE(f.fact_created_at) = d.full_date
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     d.year,
# MAGIC     d.month

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 7 — Top Customers**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_key,
# MAGIC     ROUND(SUM(total_revenue), 2) AS revenue
# MAGIC FROM fact_order_items
# MAGIC GROUP BY customer_key
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 8 — Product Revenue Ranking**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC
# MAGIC     ROUND(SUM(total_revenue), 2) AS revenue,
# MAGIC
# MAGIC     RANK() OVER (
# MAGIC         ORDER BY SUM(total_revenue) DESC
# MAGIC     ) AS revenue_rank
# MAGIC
# MAGIC FROM fact_order_items
# MAGIC
# MAGIC GROUP BY product_category