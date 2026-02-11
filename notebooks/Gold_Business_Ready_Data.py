# Databricks notebook source
# Replace these with your storage details
storage_account_name = "airbndstoraged01"       # e.g., "airbnbstorage"
storage_account_key = "*****************"
container_name = "gold"

# Spark configuration to access ADLS Gen2
spark.conf.set(f"fs.azure.account.key.airbndstoraged01.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# Read Silver Listings
silver_listings = spark.read.parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Listing/"
)

# Read Silver Reviews
silver_reviews = spark.read.parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Reviews/"
)

# Optional: Silver Calendar
silver_calendar = spark.read.parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Calendar/"
)

# Optional: Silver Location
silver_location = spark.read.parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Location/"
)

# Quick check
silver_listings.show(5)
silver_reviews.show(5)
silver_location.show(5)
silver_calendar.show(5)


# Databricks notebook source
# ------------------------------
# GOLD LAYER FROM SILVER TABLES
# ------------------------------

# ------------------------------
# 1️⃣ Read Silver tables
# ------------------------------
silver_listings = spark.read.parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Listing/"
)
silver_reviews = spark.read.parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Reviews/"
)
silver_calendar = spark.read.parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Calendar/"
)
silver_location = spark.read.parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Location/"
)

# ------------------------------
# 2️⃣ Define Gold table names
# ------------------------------
gold_tables = {
    "Gold_Listing": "default.Gold_Listing",
    "Gold_Reviews": "default.Gold_Reviews",
    "Gold_Calendar": "default.Gold_Calendar",
    "Gold_Location": "default.Gold_Location"
}

# ------------------------------
# 3️⃣ Write each Silver DataFrame to Gold as Delta
#    - overwrite schema safely
# ------------------------------
silver_listings.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_tables["Gold_Listing"])

silver_reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_tables["Gold_Reviews"])

silver_calendar.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_tables["Gold_Calendar"])

silver_location.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_tables["Gold_Location"])

# ------------------------------
