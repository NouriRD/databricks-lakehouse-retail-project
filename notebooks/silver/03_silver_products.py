# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 1 — Read Bronze Tables**

# COMMAND ----------

products_df = spark.table("nouri_retail.bronze.products_raw")

translation_df = spark.table(
    "nouri_retail.bronze.product_category_name_translation_raw"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 2 — Check Data**

# COMMAND ----------

display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 3 — Join Translation Table**

# COMMAND ----------

products_clean_df = products_df.join(
    translation_df,
    on="product_category_name",
    how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Handle Missing Values**

# COMMAND ----------

products_clean_df = products_clean_df.fillna({
    "product_category_name_english": "unknown"
})

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Rename Columns**

# COMMAND ----------

products_clean_df = products_clean_df \
    .withColumnRenamed(
        "product_category_name_english",
        "product_category"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Write Silver Table**

# COMMAND ----------

products_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("products_clean")

# COMMAND ----------

display(products_clean_df)