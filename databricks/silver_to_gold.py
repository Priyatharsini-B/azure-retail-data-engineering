from pyspark.sql.functions import sum, countDistinct, avg

# Read Silver layer data
silver_df = spark.read.format("delta") \
    .load("abfss://retail@<storage-account>.dfs.core.windows.net/silver/transactions")

# Aggregate business metrics for Gold layer
gold_df = silver_df.groupBy(
    "transaction_date",
    "product_id", "product_name", "category",
    "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)

# Write to Gold layer
gold_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@<storage-account>.dfs.core.windows.net/gold/sales_kpis")
