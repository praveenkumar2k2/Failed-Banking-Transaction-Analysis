from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("FilterFailedTransactions").getOrCreate()

# Path to cleaned data in Cloud Storage
input_path = "gs://praveen-bank/cleaned_data_single.csv"  # Update this
output_path = "gs://praveen-bank/failed_txns_filtered.csv/"  # Update this

# Read the cleaned data
df_clean = spark.read.csv(input_path, header=True, inferSchema=True)

# Filter for failed transactions
df_failed = df_clean.filter(col("status") == "FAILED")

# Save the failed transactions as CSV to a new folder
df_failed.write.csv(output_path, header=True, mode="overwrite")

print("Filtered failed transactions written to:", output_path)

# Stop the Spark session
spark.stop()
