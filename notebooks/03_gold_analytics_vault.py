from pyspark.sql.functions import col, avg, count, min, max, round

# 1. Read clean data from our Silver Delta Table
print("Reading clean data from silver_dubai_properties...")
silver_df = spark.read.table("silver_dubai_properties")

# 2. Dimensional Modeling: Create a Business Fact Table
# In production, this separates structural metrics for rapid BI dashboard queries
print("Creating Gold Business Fact Table...")
fact_properties_df = silver_df.select(
    col("event_id"),
    col("property_id"),
    col("property_type"),
    col("location"),
    col("clean_price_aed"),
    col("area_sqft"),
    col("bedrooms"),
    col("ingestion_timestamp")
)

fact_properties_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_fact_property_sales")

# 3. Aggregation Analytics: Create a Market Insights View
# Calculates average price, total listings, and price per sqft across Dubai neighborhoods
print("Generating regional real estate market insights...")
market_insights_df = fact_properties_df.groupBy("location", "property_type") \
    .agg(
        count("property_id").alias("total_listings"),
        round(avg("clean_price_aed"), 2).alias("avg_price_aed"),
        round(avg(col("clean_price_aed") / col("area_sqft")), 2).alias("avg_price_per_sqft"),
        min("clean_price_aed").alias("min_price_aed"),
        max("clean_price_aed").alias("max_price_aed")
    ).orderBy("location", col("avg_price_aed").desc())

market_insights_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_market_insights")

print("Gold analytical tables successfully written!")

# 4. Enterprise Feature Showcase: Demonstrate Delta Lake Time Travel
# This proves to a recruiter you understand data version control and governance
print("\n--- Showcase: Delta Table History (Time Travel) ---")
delta_table_history = spark.sql("DESCRIBE HISTORY silver_dubai_properties")
display(delta_table_history.select("version", "timestamp", "operation", "operationParameters"))
