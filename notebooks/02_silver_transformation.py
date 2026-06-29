from pyspark.sql.functions import col, regexp_replace, expr, row_number, when
from pyspark.sql.window import Window

# 1. Read the messy data from our Bronze Delta Table
print("Reading raw data from bronze_dubai_properties...")
raw_bronze_df = spark.read.table("bronze_dubai_properties")

# 2. Problem Solver: Standardize and clean the 'raw_price' column
# We strip ' AED', handle 'M' multipliers, and cast to a pure DoubleType numeric field
cleaned_price_df = raw_bronze_df.withColumn(
    "clean_price_aed",
    when(col("raw_price").contains("M"), 
         regexp_replace(col("raw_price"), "M", "").cast("double") * 1000000)
    .otherwise(regexp_replace(col("raw_price"), " AED", "").cast("double"))
)

# 3. Problem Solver: Deduplicate late-arriving records using Window functions
# We partition by property_id and keep only the absolute latest event based on the timestamp
window_spec = Window.partitionBy("property_id").orderBy(col("ingestion_timestamp").desc())

deduplicated_silver_df = cleaned_price_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num", "raw_price") # Drop structural sorting column and the old messy price column

# 4. Write the clean data to the Silver Layer Table
silver_table_name = "silver_dubai_properties"
print("Writing sanitized data to Silver Delta Lake...")

deduplicated_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(silver_table_name)

print(f"Sanitized data successfully written to: {silver_table_name}")

# Show a sample of our clean, standardized, and deduplicated Silver data
display(spark.sql(f"SELECT * FROM {silver_table_name} LIMIT 10"))
