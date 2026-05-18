# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

items_df = spark.table(
    "nouri_retail.silver.order_items_clean"
)

orders_df = spark.table(
    "nouri_retail.silver.orders_clean"
)

payments_df = spark.table(
    "nouri_retail.silver.payments_clean"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Read Dimensions**

# COMMAND ----------

dim_products_df = spark.table(
    "nouri_retail.gold.dim_products"
)

dim_customers_df = spark.table(
    "nouri_retail.gold.dim_customers"
)

dim_sellers_df = spark.table(
    "nouri_retail.gold.dim_sellers"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Join Orders**

# COMMAND ----------

fact_df = items_df.join(
    orders_df,
    on="order_id",
    how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Join Payments**

# COMMAND ----------

fact_df = fact_df.join(
    payments_df,
    on="order_id",
    how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Join Product Dimension**

# COMMAND ----------

fact_df = fact_df.join(
    dim_products_df.select(
        "product_id",
        "product_key",
        "product_category"
    ),
    on="product_id",
    how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 8 — Join Customer Dimension**

# COMMAND ----------

fact_df = fact_df.join(
    dim_customers_df.select(
        "customer_id",
        "customer_key",
        "customer_state"
    ),
    on="customer_id",
    how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 9 — Join Seller Dimension**

# COMMAND ----------

fact_df = fact_df.join(
    dim_sellers_df.select(
        "seller_id",
        "seller_key",
        "seller_state"
    ),
    on="seller_id",
    how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 10 — Add Fact Metrics**

# COMMAND ----------

fact_df = fact_df \
    .withColumn(
        "total_revenue",
        col("payment_value")
    ) \
    .withColumn(
        "shipping_cost",
        col("freight_value")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 11 — Add Metadata**

# COMMAND ----------

fact_df = fact_df.withColumn(
    "fact_created_at",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 12 — Select Final Columns**

# COMMAND ----------

fact_df = fact_df.withColumn(
    "fact_created_at",
    current_timestamp()
)

# COMMAND ----------

fact_df = fact_df.select(
    "order_id",
    "order_item_id",

    "customer_key",
    "product_key",
    "seller_key",

    "product_category",
    "customer_state",
    "seller_state",

    "payment_type",
    "payment_installments",

    "price",
    "freight_value",
    "shipping_cost",
    "total_revenue",

    "delivery_days",
    "is_delivered",

    "fact_created_at"
)

# COMMAND ----------

fact_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("fact_order_items")

# COMMAND ----------

display(fact_df)