# Databricks notebook source
# MAGIC %md
# MAGIC **STEP 1 — Switch to Silver Schema**

# COMMAND ----------

spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 2 — Read Bronze Table**

# COMMAND ----------

orders_df = spark.table("nouri_retail.bronze.orders_raw")

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 3 — Real Transformations**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

orders_clean_df = orders_df \
    .withColumn(
        "order_purchase_timestamp",
        to_timestamp("order_purchase_timestamp")
    ) \
    .withColumn(
        "order_approved_at",
        to_timestamp("order_approved_at")
    ) \
    .withColumn(
        "order_delivered_carrier_date",
        to_timestamp("order_delivered_carrier_date")
    ) \
    .withColumn(
        "order_delivered_customer_date",
        to_timestamp("order_delivered_customer_date")
    ) \
    .withColumn(
        "order_estimated_delivery_date",
        to_timestamp("order_estimated_delivery_date")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Add Business Columns**

# COMMAND ----------

orders_clean_df = orders_clean_df \
    .withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    ) \
    .withColumn(
        "is_delivered",
        when(col("order_status") == "delivered", 1).otherwise(0)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Write Silver Table**

# COMMAND ----------

orders_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("orders_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Verify Silver Table**

# COMMAND ----------

display(spark.sql("""
SELECT *
FROM orders_clean
LIMIT 10
"""))

# COMMAND ----------

