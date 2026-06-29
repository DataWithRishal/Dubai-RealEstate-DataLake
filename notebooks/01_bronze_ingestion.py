from pyspark.sql.functions import expr, current_timestamp, rand, round, concat, lit

# 1. Generate 10 Million Records using Spark's native distributed engine
record_count = 10000000
print(f"Generating {record_count} raw property events...")

# Using spark.range is highly optimized for Databricks distributed compute
bronze_df = spark.range(0, record_count).select(
    expr("uuid()").alias("event_id"),
    expr("uuid()").alias("property_id"),
    expr("""
        CASE 
            WHEN rand() < 0.4 THEN 'Apartment' 
            WHEN rand() < 0.7 THEN 'Villa' 
            WHEN rand() < 0.9 THEN 'Townhouse' 
            ELSE 'Penthouse' 
        END
    """).alias("property_type"),
    expr("""
        CASE 
            WHEN rand() < 0.3 THEN 'Downtown Dubai' 
            WHEN rand() < 0.6 THEN 'Dubai Marina' 
            WHEN rand() < 0.8 THEN 'Jumeirah Village Circle (JVC)' 
            ELSE 'Palm Jumeirah' 
        END
    """).alias("location"),
    # Injecting messy currency formats (Problem 2)
    expr("""
        CASE 
            WHEN rand() < 0.2 THEN concat(cast(round(rand() * 10000000 + 500000, 0) as string), ' AED')
            WHEN rand() < 0.3 THEN concat(cast(round((rand() * 10000000 + 500000)/1000000, 1) as string), 'M')
            ELSE cast(round(rand() * 10000000 + 500000, 0) as string)
        END
    """).alias("raw_price"),
    round(rand() * 5000 + 400, 0).alias("area_sqft"),
    expr("cast(rand() * 6 + 1 as int)").alias("bedrooms"),
    current_timestamp().alias("ingestion_timestamp")
)

# 2. Inject deliberate duplicates (Problem 1) to simulate late-arriving data
duplicate_df = bronze_df.sample(fraction=0.05) 
final_bronze_df = bronze_df.union(duplicate_df)

# 3. Write to Delta Lake (Append Only)
table_name = "bronze_dubai_properties"

print("Writing 10.5M records to Delta Lake...")
final_bronze_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(table_name)

print(f"Successfully ingested data into Delta table: {table_name}")

# Display a sample of the messy data
display(spark.sql(f"SELECT * FROM {table_name} LIMIT 10"))
