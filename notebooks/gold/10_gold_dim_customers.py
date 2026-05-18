# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

customers_df = spark.table(
    "nouri_retail.silver.customers_clean"
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Create Surrogate Key**

# COMMAND ----------

window_spec = Window.orderBy("customer_id")

dim_customers_df = customers_df.withColumn(
    "customer_key",
    row_number().over(window_spec)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Select Final Columns**

# COMMAND ----------

dim_customers_df = dim_customers_df.select(
    "customer_key",
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "customer_zip_code_prefix",
    "ingestion_date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Write Gold Table**

# COMMAND ----------

dim_customers_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_customers")

# COMMAND ----------

display(dim_customers_df)