# Databricks notebook source
# Replace these with your storage details
storage_account_name = "airbndstoraged01"       # e.g., "airbnbstorage"
storage_account_key = "**************************"
container_name = "bronze"

# Spark configuration to access ADLS Gen2
spark.conf.set(f"fs.azure.account.key.airbndstoraged01.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, to_date, coalesce
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

from pyspark.sql.functions import col, to_date, regexp_replace, coalesce

calendar_df = spark.read.option("header", True).csv(
    "abfss://bronze@airbndstoraged01.dfs.core.windows.net/Calendar/"
)

# View first 5 rows
print("=== Bronze Calendar ===")
calendar_df.show(5)

# View schema
calendar_df.printSchema()


# COMMAND ----------

calendar_clean = calendar_df \
    .withColumn("listing_id", col("listing_id").cast("int")) \
    .withColumn("date", to_date(col("date"))) \
    .withColumn("price", regexp_replace(col("price"), "[$,]", "").cast("double"))

print("=== Cleaned Calendar ===")
calendar_clean.show(5)
calendar_clean.printSchema()


# COMMAND ----------

# Read listings from Bronze to fill missing price
listings_df = spark.read.option("header", True).csv(
    "abfss://bronze@airbndstoraged01.dfs.core.windows.net/Listings/"
)

calendar_joined = calendar_clean.join(
    listings_df.select("id","price").withColumnRenamed("id","listing_id")
        .withColumnRenamed("price","listing_price"),
    "listing_id",
    "left"
)

print("=== Calendar Joined with Listings ===")
calendar_joined.show(5)
calendar_joined.printSchema()


# COMMAND ----------

silver_calendar = calendar_joined \
    .withColumn("final_price", coalesce(col("price"), col("listing_price"))) \
    .select("listing_id", "date", "final_price")

print("=== Silver Calendar (Final) ===")
silver_calendar.show(10)
silver_calendar.printSchema()

# COMMAND ----------

# Save Silver Reviews (for analytics)
silver_calendar.write.mode("overwrite").parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Calendar/"
)

# COMMAND ----------

from pyspark.sql.functions import col, to_date

review_details_df = spark.read.option("header", True).csv(
    "abfss://bronze@airbndstoraged01.dfs.core.windows.net/Review_details/"
)

# Quick view
print("=== Bronze Reviews ===")
review_details_df.show(5)
review_details_df.printSchema()

# COMMAND ----------

# Clean
silver_reviews = review_details_df \
    .filter(col("listing_id").rlike("^[0-9]+$")) \
    .withColumn("listing_id", col("listing_id").cast("int")) \
    .withColumn("review_date", to_date(col("date"))) \
    .withColumn("reviewer_id", col("reviewer_id").cast("int")) \
    .select("listing_id", "review_date", "reviewer_id") \
    .dropna(subset=["listing_id", "review_date"])

# View cleaned Silver Reviews
silver_reviews.show(10)
silver_reviews.printSchema()

# COMMAND ----------

# Save Silver Reviews (for analytics)
silver_reviews.write.mode("overwrite").parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Reviews/"
)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, when

# ------------------------------
# 1️⃣ Load Bronze Review Details
# ------------------------------
review_details_df = spark.read.option("header", True) \
    .option("multiLine", True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("inferSchema", True) \
    .csv("abfss://bronze@airbndstoraged01.dfs.core.windows.net/Review_details/")

# ------------------------------
# 2️⃣ Load Listing Details
# ------------------------------
listing_details_df = spark.read.option("header", True) \
    .option("multiLine", True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("inferSchema", True) \
    .csv("abfss://bronze@airbndstoraged01.dfs.core.windows.net/Listing_Details/")

# ------------------------------
# 3️⃣ Clean Silver Reviews
# ------------------------------
silver_reviews = review_details_df \
    .filter(col("listing_id").rlike("^[0-9]+$")) \
    .withColumn("listing_id", col("listing_id").cast("int")) \
    .withColumn("review_date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("reviewer_id", col("reviewer_id").cast("int")) \
    .select("listing_id", "review_date", "reviewer_id") \
    .dropna(subset=["listing_id", "review_date"])

# ------------------------------
# 4️⃣ Clean Listing Ratings
# ------------------------------
listing_ratings_clean = listing_details_df \
    .withColumn("listing_id", col("id").cast("int")) \
    .withColumn("review_score", regexp_replace(col("review_scores_rating"), "[$,]", "")) \
    .withColumn("review_score", col("review_score").cast("double")) \
    .select("listing_id", "review_score")

# ------------------------------
# 5️⃣ Join Reviews with Ratings
# ------------------------------
silver_reviews_with_rating = silver_reviews.join(
    listing_ratings_clean,
    on="listing_id",
    how="left"
)

# Optional: fill missing review_score with None (already NULL if not numeric)
silver_reviews_with_rating = silver_reviews_with_rating.withColumn(
    "review_score",
    when(col("review_score").isNull(), None).otherwise(col("review_score"))
)

# ------------------------------
# 6️⃣ View cleaned Silver Reviews
# ------------------------------
silver_reviews_with_rating.show(20, truncate=False)
silver_reviews_with_rating.printSchema()

# ------------------------------
# 7️⃣ Save to Silver Layer
# ------------------------------
silver_reviews_with_rating.write.mode("overwrite").parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Reviews/"
)


# COMMAND ----------

from pyspark.sql.functions import col, to_date, regexp_replace

# Read Bronze Listing_Details
listing_details_df = spark.read.option("header", True).csv(
    "abfss://bronze@airbndstoraged01.dfs.core.windows.net/Listing_Details/"
)

# Clean Silver Listings
silver_listings = listing_details_df \
    .filter(col("id").rlike("^[0-9]+$")) \
    .withColumn("listing_id", col("id").cast("int")) \
    .withColumn("host_id", col("host_id").cast("int")) \
    .withColumn("price", regexp_replace(col("price"), "[$,]", "").cast("int")) \
    .withColumn("minimum_nights", col("minimum_nights").cast("int")) \
    .withColumn("availability_365", col("availability_365").cast("int")) \
    .withColumn("number_of_reviews", col("number_of_reviews").cast("int")) \
    .withColumn("accommodates", col("accommodates").cast("int")) \
    .withColumn("bathrooms", col("bathrooms").cast("float")) \
    .withColumn("bedrooms", col("bedrooms").cast("int")) \
    .withColumn("beds", col("beds").cast("int")) \
    .withColumn("review_scores_rating", col("review_scores_rating").cast("float")) \
    .withColumn("last_review", to_date(col("last_review"))) \
    .withColumn("latitude", col("latitude").cast("double")) \
    .withColumn("longitude", col("longitude").cast("double")) \
    .select(
        "listing_id", "host_id", "room_type", "neighbourhood_cleansed",
        "price", "minimum_nights", "availability_365", "number_of_reviews",
        "last_review", "accommodates", "bathrooms", "bedrooms", "beds",
        "review_scores_rating", "latitude", "longitude"
    ) \
    .dropna(subset=["listing_id", "price", "latitude", "longitude"])


# View cleaned Silver Listings
silver_listings.show(10)
silver_listings.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col

# Keep only valid listings
silver_listings_clean = silver_listings.filter(
    (col("listing_id").isNotNull()) &
    (col("price").isNotNull()) &
    (col("room_type").isin("Entire home/apt", "Private room", "Shared room", "Hotel room")) &
    (col("neighbourhood_cleansed").isNotNull())
)

# Fill numeric nulls with defaults
silver_listings_clean = silver_listings_clean.fillna({
    "minimum_nights": 1,
    "availability_365": 0,
    "number_of_reviews": 0,
    "accommodates": 1,
    "bathrooms": 1.0,
    "bedrooms": 1,
    "beds": 1,
    "review_scores_rating": 0.0
})

# Quick check
silver_listings_clean.show(10)
silver_listings_clean.printSchema()


# COMMAND ----------

# Save as Parquet (recommended)
silver_listings_clean.write.mode("overwrite").parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Listing/"
)




# COMMAND ----------

# DBTITLE 1,Cell 16
from pyspark.sql.functions import col, trim, regexp_replace, monotonically_increasing_id, lit, when

# Select relevant columns
location_df = silver_listings_clean.select(
    col("listing_id"),
    trim(col("neighbourhood_cleansed")).alias("neighbourhood"),
    col("latitude").cast("double").alias("latitude"),
    col("longitude").cast("double").alias("longitude")
).dropDuplicates(["listing_id"])

# Fill null neighbourhood_group with "Amsterdam"
location_df = location_df.withColumn(
    "neighbourhood_group",
    lit("Amsterdam")
)

# Clean text
location_df = location_df.withColumn("neighbourhood", regexp_replace(col("neighbourhood"), r"\s+", " "))
location_df = location_df.withColumn("neighbourhood_group", regexp_replace(col("neighbourhood_group"), r"\s+", " "))

# Filter invalid rows
location_df = location_df.filter(
    (col("neighbourhood").isNotNull()) &
    (col("latitude").isNotNull()) &
    (col("longitude").isNotNull())
)

# Add surrogate key
location_df = location_df.withColumn("location_id", monotonically_increasing_id())

# Preview
location_df.show(20)


# Save to Silver layer
location_df.write.mode("overwrite").parquet(
    "abfss://silver@airbndstoraged01.dfs.core.windows.net/Silver_Location/"
)