# Databricks notebook source
# MAGIC %md
# MAGIC ### FETCHING DATA FROM AZURE DATA LAKE

# COMMAND ----------

# Replace these with your storage details
storage_account_name = "airbndstoraged01"       # e.g., "airbnbstorage"
storage_account_key = "************************************"
container_name = "bronze"


# Spark configuration to access ADLS Gen2
spark.conf.set(f"fs.azure.account.key.airbndstoraged01.dfs.core.windows.net", storage_account_key)



# COMMAND ----------

dbutils.fs.ls("abfss://bronze@airbndstoraged01.dfs.core.windows.net/")


# COMMAND ----------

# Calendar
calendar_df = spark.read.option("header", True).csv(
    "abfss://bronze@airbndstoraged01.dfs.core.windows.net/Calendar/"
)
calendar_df.show(5)

listings_df = spark.read.option("header", True).csv(
    "abfss://bronze@airbndstoraged01.dfs.core.windows.net/Listings/"
)

listing_details_df = spark.read.option("header", True).csv(
    "abfss://bronze@airbndstoraged01.dfs.core.windows.net/Listing_Details/"
)

reviews_df = spark.read.option("header", True).csv(
    "abfss://bronze@airbndstoraged01.dfs.core.windows.net/Reviews/"
)

review_details_df = spark.read.option("header", True).csv(
    "abfss://bronze@airbndstoraged01.dfs.core.windows.net/Review_details/"
)



# COMMAND ----------

calendar_df.printSchema()
calendar_df.show(5)


# COMMAND ----------

calendar_df.select("price").distinct().show()


# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import regexp_replace, col, to_date

calendar_silver_df = calendar_df \
    .withColumn("listing_id", col("listing_id").cast("int")) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("price", regexp_replace(col("price"), "[$,]", "").cast("double"))


# COMMAND ----------

calendar_silver_df.select("listing_id", "date", "price").show(20)


# COMMAND ----------

listings_df.printSchema()
listings_df.show(5)

# COMMAND ----------

review_details_df.printSchema()
review_details_df.show(5)

# COMMAND ----------

listing_details_df.printSchema()
listing_details_df.show(5)