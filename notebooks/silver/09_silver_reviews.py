# Databricks notebook source
spark.sql("USE CATALOG nouri_retail")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

reviews_df = spark.table(
    "nouri_retail.bronze.order_reviews_raw"
)

display(reviews_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 4 — Handle Missing Text**

# COMMAND ----------

reviews_clean_df = reviews_df.fillna({
    "review_comment_title": "No Title",
    "review_comment_message": "No Comment"
})

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 — Cast Dates**

# COMMAND ----------

reviews_clean_df = reviews_clean_df \
    .withColumn(
        "review_creation_date",
        expr("try_cast(review_creation_date as timestamp)")
    ) \
    .withColumn(
        "review_answer_timestamp",
        expr("try_cast(review_answer_timestamp as timestamp)")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 6 — Add Review Label**

# COMMAND ----------

reviews_clean_df = reviews_clean_df.withColumn(
    "review_score",
    expr("try_cast(review_score as int)")
)

# COMMAND ----------

reviews_clean_df = reviews_clean_df.withColumn(
    "review_label",
    when(col("review_score") >= 4, "positive")
    .when(col("review_score") == 3, "neutral")
    .otherwise("negative")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 — Add Metadata**

# COMMAND ----------

reviews_clean_df = reviews_clean_df.withColumn(
    "ingestion_date",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 8 — Remove Duplicates**

# COMMAND ----------

reviews_clean_df = reviews_clean_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 9 — Write Silver Table**

# COMMAND ----------

reviews_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("reviews_clean")

# COMMAND ----------

display(reviews_clean_df)