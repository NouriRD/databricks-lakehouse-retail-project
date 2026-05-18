# Databricks notebook source
# MAGIC %md
# MAGIC **STEP 1 — Set Active Catalog & Schema**

# COMMAND ----------

spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 2 — Read CSV from Volume**

# COMMAND ----------

orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/nouri_retail/bronze/raw_files/olist_orders_dataset.csv")

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 3 — Create Bronze Delta Table**

# COMMAND ----------

orders_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("orders_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Verify Table**

# COMMAND ----------

display(spark.sql("SELECT * FROM orders_raw LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Multi-table Bronze Ingestion**

# COMMAND ----------

tables = [
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation"
]

# COMMAND ----------

for table in tables:

    file_path = f"/Volumes/nouri_retail/bronze/raw_files/{table}.csv"

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)

    bronze_table_name = table.replace("olist_", "").replace("_dataset", "") + "_raw"

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(bronze_table_name)

    print(f"Created table: {bronze_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Verify All Bronze Tables**

# COMMAND ----------

display(spark.sql("SHOW TABLES"))