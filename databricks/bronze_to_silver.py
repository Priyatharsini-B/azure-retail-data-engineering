# Read Bronze data
base_path = "abfss://retail@retailstoragepoc.dfs.core.windows.net"

df_transactions = spark.read.parquet(f"{base_path}/bronze/transaction/")
df_products     = spark.read.parquet(f"{base_path}/bronze/product/")
df_stores       = spark.read.parquet(f"{base_path}/bronze/store/")
df_customers    = spark.read.parquet(f"{base_path}/bronze/customer/customers.parquet")

display(df_transactions)


# Basic cleansing
from pyspark.sql.functions import col

df_transactions_silver = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

display(df_transactions_silver)

df_products_silver = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

display(df_products_silver)

df_stores_silver = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

display(df_stores_silver)

df_customers_silver = df_customers.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "registration_date"
).dropDuplicates(["customer_id"])

display(df_customers_silver)

from pyspark.sql.functions import col

df_silver = df_transactions_silver \
    .join(df_customers_silver, "customer_id", "inner") \
    .join(df_products_silver, "product_id", "inner") \
    .join(df_stores_silver, "store_id", "inner") \
    .withColumn("total_amount", col("quantity") * col("price"))

df_silver = df_transactions_silver \
    .join(df_customers_silver, "customer_id", "inner") \
    .join(df_products_silver, "product_id", "inner") \
    .join(df_stores_silver, "store_id", "inner") \
    .withColumn("total_amount", col("quantity") * col("price"))


# Write to Silver layer
silver_path = "abfss://retail@retailstoragepoc.dfs.core.windows.net/silver/"

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .save(silver_path)
