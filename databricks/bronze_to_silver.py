from pyspark.sql.functions import col, current_date

# Read Bronze data
bronze_df = spark.read.format("parquet") \
    .load("abfss://retail@<storage-account>.dfs.core.windows.net/bronze/transactions")

# Basic cleansing
silver_df = bronze_df \
    .dropDuplicates() \
    .filter(col("sales_amount").isNotNull()) \
    .withColumn("ingestion_date", current_date())

# Write to Silver layer
silver_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@<storage-account>.dfs.core.windows.net/silver/transactions")
