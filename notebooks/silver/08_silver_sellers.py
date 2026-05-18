# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sellers_df = spark.table(
    "nouri_retail.bronze.sellers_raw"
)

display(sellers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Standardize Columns**

# COMMAND ----------

sellers_clean_df = sellers_df \
    .withColumn(
        "seller_city",
        initcap(trim(col("seller_city")))
    ) \
    .withColumn(
        "seller_state",
        upper(trim(col("seller_state")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Remove Duplicates**

# COMMAND ----------

sellers_clean_df = sellers_clean_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Add Metadata**

# COMMAND ----------

sellers_clean_df = sellers_clean_df.withColumn(
    "ingestion_date",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Write Silver Table**

# COMMAND ----------

sellers_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("sellers_clean")

# COMMAND ----------

display(sellers_clean_df)