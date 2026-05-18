# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

sellers_df = spark.table(
    "nouri_retail.silver.sellers_clean"
)

display(sellers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Create Surrogate Key**

# COMMAND ----------

window_spec = Window.orderBy("seller_id")

dim_sellers_df = sellers_df.withColumn(
    "seller_key",
    row_number().over(window_spec)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Add Metadata**

# COMMAND ----------

dim_sellers_df = dim_sellers_df.withColumn(
    "ingestion_date",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Select Final Columns**

# COMMAND ----------

dim_sellers_df = dim_sellers_df.select(
    "seller_key",
    "seller_id",
    "seller_city",
    "seller_state",
    "seller_zip_code_prefix",
    "ingestion_date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Write Gold Table**

# COMMAND ----------

dim_sellers_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_sellers")

# COMMAND ----------

display(dim_sellers_df)