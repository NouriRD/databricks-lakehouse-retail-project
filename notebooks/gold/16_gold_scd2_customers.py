# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

customers_df = spark.table(
    "nouri_retail.gold.dim_customers"
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Simulate Customer Change**

# COMMAND ----------

updated_customers_df = customers_df.withColumn(
    "customer_state",
    when(
        col("customer_id") == "3ce436f183e68e07877b285a838db11a",
        "RJ"
    ).otherwise(col("customer_state"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Add SCD Columns**

# COMMAND ----------

scd_df = updated_customers_df \
    .withColumn(
        "start_date",
        current_date()
    ) \
    .withColumn(
        "end_date",
        lit(None).cast("date")
    ) \
    .withColumn(
        "is_current",
        lit(1)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Create Historical Version**

# COMMAND ----------

historical_df = customers_df \
    .filter(
        col("customer_id") ==
        "3ce436f183e68e07877b285a838db11a"
    ) \
    .withColumn(
        "end_date",
        current_date()
    ) \
    .withColumn(
        "is_current",
        lit(0)
    ) \
    .withColumn(
        "start_date",
        lit("2024-01-01").cast("date")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Union Historical + Current**

# COMMAND ----------

final_scd_df = scd_df.unionByName(
    historical_df,
    allowMissingColumns=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 8 — Write SCD Table**

# COMMAND ----------

final_scd_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_customers_scd2")

# COMMAND ----------

display(final_scd_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

