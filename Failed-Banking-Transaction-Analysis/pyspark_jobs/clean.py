'''from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = SparkSession.builder.appName("BankTxnCleaning").getOrCreate()

df = spark.read.csv("gs://revp1-bucket/*.csv", header=True, inferSchema=True)
df_clean = df.dropna().filter((trim(col("transaction_id")) != "") & (trim(col("status")) != ""))
df_clean.write.csv("gs://revp1-bucket/cleaned_data.csv", header=True, mode="overwrite")'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# Initialize Spark session
spark = SparkSession.builder.appName("BankTxnCleaning").getOrCreate()

# Read all CSV files from the GCS bucket
df = spark.read.option("recursiveFileLookup", "true").csv("gs://praveen-bank/raw_data/", header=True, inferSchema=True)

# Clean the data: Remove rows with missing or empty transaction_id and status
df_clean = df.dropna().filter((trim(col("transaction_id")) != "") & (trim(col("status")) != ""))

# Save the cleaned data back to GCS 
df_clean.coalesce(1).write.csv("gs://praveen-bank/cleaned_data_single.csv", header=True, mode="overwrite")


print("Data cleaning completed. Cleaned files saved to GCS.")

# Stop the Spark session
spark.stop()

