# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 3 — Check Table History**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY fact_order_items

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Time Travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fact_order_items VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Create Update Example**

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fact_order_items
# MAGIC SET total_revenue = total_revenue * 1.1
# MAGIC WHERE payment_type = 'credit_card'

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Check New Version**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY fact_order_items

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Compare Old vs New**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     AVG(total_revenue)
# MAGIC FROM fact_order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     AVG(total_revenue)
# MAGIC FROM fact_order_items VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 8 — Create Incoming Updates**

# COMMAND ----------

updates_df = spark.createDataFrame([
    (
        "new_order",
        9999.0
    )
], ["order_id", "total_revenue"])

display(updates_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 9 — Create Temp View**

# COMMAND ----------

updates_df.createOrReplaceTempView(
    "updates_view"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 10 — MERGE INTO**

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO fact_order_items f
# MAGIC USING updates_view u
# MAGIC
# MAGIC ON f.order_id = u.order_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     f.total_revenue = u.total_revenue
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     order_id,
# MAGIC     total_revenue
# MAGIC )
# MAGIC VALUES (
# MAGIC     u.order_id,
# MAGIC     u.total_revenue
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 11 — OPTIMIZE**

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE fact_order_items

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 12 — VACUUM**

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM fact_order_items RETAIN 168 HOURS