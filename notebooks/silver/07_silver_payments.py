# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

payments_df = spark.table(
    "nouri_retail.bronze.order_payments_raw"
)

display(payments_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Standardize Payment Type**

# COMMAND ----------

payments_clean_df = payments_df.withColumn(
    "payment_type",
    lower(trim(col("payment_type")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Cast Numeric Columns**

# COMMAND ----------

payments_clean_df = payments_clean_df \
    .withColumn(
        "payment_installments",
        col("payment_installments").cast("int")
    ) \
    .withColumn(
        "payment_value",
        col("payment_value").cast("double")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Add Business Columns**

# COMMAND ----------

payments_clean_df = payments_clean_df.withColumn(
    "is_installment",
    when(col("payment_installments") > 1, 1).otherwise(0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Add Metadata**

# COMMAND ----------

payments_clean_df = payments_clean_df.withColumn(
    "ingestion_date",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 8 — Remove Duplicates**

# COMMAND ----------

payments_clean_df = payments_clean_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 9 — Write Silver Table**

# COMMAND ----------

payments_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("payments_clean")

# COMMAND ----------

display(payments_clean_df)

# COMMAND ----------



# COMMAND ----------

