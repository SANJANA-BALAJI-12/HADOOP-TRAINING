# -----------------------------------
# 1. START SPARK
# -----------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder \
    .appName("CapstoneCustomerStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------
# 2. LOAD STATIC CUSTOMER DATA
# -----------------------------------
customer_df = spark.read.csv(
    "hdfs:///user/cloudera/CAPSTONE/customer.csv",
    header=True,
    inferSchema=True
)

# -----------------------------------
# 3. DEFINE TRANSACTION SCHEMA
# -----------------------------------
transaction_schema = "txn_id INT, customer_id INT, amount INT"

# -----------------------------------
# 4. READ STREAMING TRANSACTIONS
# -----------------------------------
transactions_df = spark.readStream \
    .schema(transaction_schema) \
    .option("header", "true") \
    .csv("hdfs:///user/cloudera/CAPSTONE/")

# -----------------------------------
# 5. JOIN WITH CUSTOMER DATA
# -----------------------------------
joined_df = transactions_df.join(customer_df, "customer_id")

# -----------------------------------
# 6. ANALYTICS
# -----------------------------------
# Total Spending per Customer
total_spending = joined_df.groupBy("customer_id", "name") \
    .agg(sum("amount").alias("TotalSpent"))

# High-Value Transactions (>3000)
high_value = joined_df.filter(col("amount") > 3000) \
    .select("txn_id", "name", "amount")

# Live Transaction Count
transaction_count = joined_df.groupBy().agg(count("*").alias("TotalTransactions"))

# City-wise Spending
city_spending = joined_df.groupBy("city").agg(sum("amount").alias("TotalSpending"))

# -----------------------------------
# 7. OUTPUT TO CONSOLE
# -----------------------------------
query1 = total_spending.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query2 = high_value.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query3 = transaction_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query4 = city_spending.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# -----------------------------------
# 8. KEEP STREAM RUNNING
# -----------------------------------
spark.streams.awaitAnyTermination()

