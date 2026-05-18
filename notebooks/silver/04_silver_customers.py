# Databricks notebook source
# MAGIC %md
# MAGIC **STEP 1 — Read Bronze Table**

# COMMAND ----------

spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

customers_df = spark.table(
    "nouri_retail.bronze.customers_raw"
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 2 — Standardize Text Columns**

# COMMAND ----------

customers_clean_df = customers_df \
    .withColumn(
        "customer_city",
        initcap(trim(col("customer_city")))
    ) \
    .withColumn(
        "customer_state",
        upper(trim(col("customer_state")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 3 — Remove Duplicates**

# COMMAND ----------

customers_clean_df = customers_clean_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Add Ingestion Timestamp**

# COMMAND ----------

customers_clean_df = customers_clean_df.withColumn(
    "ingestion_date",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Write Silver Table**

# COMMAND ----------

customers_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("customers_clean")

# COMMAND ----------

display(customers_clean_df)

# COMMAND ----------

