# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

items_df = spark.table(
    "nouri_retail.bronze.order_items_raw"
)

display(items_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Cast Numeric Columns**

# COMMAND ----------

items_clean_df = items_df \
    .withColumn(
        "price",
        col("price").cast("double")
    ) \
    .withColumn(
        "freight_value",
        col("freight_value").cast("double")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Add Total Amount**

# COMMAND ----------

items_clean_df = items_clean_df.withColumn(
    "total_amount",
    col("price") + col("freight_value")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Standard Metadata**

# COMMAND ----------

items_clean_df = items_clean_df.withColumn(
    "ingestion_date",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Remove Duplicates**

# COMMAND ----------

items_clean_df = items_clean_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 8 — Write Silver Table**

# COMMAND ----------

items_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("order_items_clean")

# COMMAND ----------

display(items_clean_df)

# COMMAND ----------

