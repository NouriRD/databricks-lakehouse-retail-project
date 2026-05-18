# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

products_df = spark.table(
    "nouri_retail.silver.products_clean"
)

display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Create Surrogate Key**

# COMMAND ----------

window_spec = Window.orderBy("product_id")

dim_products_df = products_df \
    .withColumn(
        "product_key",
        row_number().over(window_spec)
    ) \
    .withColumn(
        "product_volume_cm3",
        col("product_length_cm") *
        col("product_height_cm") *
        col("product_width_cm")
    ) \
    .withColumn(
        "ingestion_date",
        current_timestamp()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Select Final Columns**

# COMMAND ----------

dim_products_df = dim_products_df.select(
    "product_key",
    "product_id",
    "product_category",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "product_volume_cm3",
    "ingestion_date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Write Gold Table**

# COMMAND ----------

dim_products_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_products")

# COMMAND ----------

display(dim_products_df)

# COMMAND ----------



# COMMAND ----------

