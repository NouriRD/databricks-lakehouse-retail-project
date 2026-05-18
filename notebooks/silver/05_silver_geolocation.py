# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 3 — Read Bronze Table**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

geo_df = spark.table(
    "nouri_retail.bronze.geolocation_raw"
)

display(geo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Standardize Text**

# COMMAND ----------

geo_clean_df = geo_df \
    .withColumn(
        "geolocation_city",
        initcap(trim(col("geolocation_city")))
    ) \
    .withColumn(
        "geolocation_state",
        upper(trim(col("geolocation_state")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Real Deduplication **

# COMMAND ----------

geo_clean_df = geo_clean_df.dropDuplicates([
    "geolocation_zip_code_prefix",
    "geolocation_city",
    "geolocation_state"
])

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Add Metadata**

# COMMAND ----------

geo_clean_df = geo_clean_df.withColumn(
    "ingestion_date",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Write Silver Table**

# COMMAND ----------

geo_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("geolocation_clean")

# COMMAND ----------

display(geo_clean_df)