# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 3 — Read Orders Table**

# COMMAND ----------

orders_df = spark.table(
    "nouri_retail.silver.orders_clean"
)

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Extract Unique Dates**

# COMMAND ----------

dim_date_df = orders_df.select(
    to_date(col("order_purchase_timestamp"))
    .alias("full_date")
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Create Date Attributes**

# COMMAND ----------

dim_date_df = dim_date_df \
    .withColumn(
        "date_key",
        date_format(
            col("full_date"),
            "yyyyMMdd"
        ).cast("int")
    ) \
    .withColumn(
        "year",
        year(col("full_date"))
    ) \
    .withColumn(
        "month",
        month(col("full_date"))
    ) \
    .withColumn(
        "month_name",
        date_format(
            col("full_date"),
            "MMMM"
        )
    ) \
    .withColumn(
        "quarter",
        quarter(col("full_date"))
    ) \
    .withColumn(
        "week_of_year",
        weekofyear(col("full_date"))
    ) \
    .withColumn(
        "day_of_month",
        dayofmonth(col("full_date"))
    ) \
    .withColumn(
        "day_name",
        date_format(
            col("full_date"),
            "EEEE"
        )
    ) \
    .withColumn(
        "is_weekend",
        when(
            dayofweek(col("full_date")).isin(1,7),
            1
        ).otherwise(0)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Add Metadata**

# COMMAND ----------

dim_date_df = dim_date_df.withColumn(
    "created_at",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Select Final Columns**

# COMMAND ----------

dim_date_df = dim_date_df.select(
    "date_key",
    "full_date",
    "year",
    "quarter",
    "month",
    "month_name",
    "week_of_year",
    "day_of_month",
    "day_name",
    "is_weekend",
    "created_at"
)

# COMMAND ----------

dim_date_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_date")

# COMMAND ----------

display(dim_date_df)