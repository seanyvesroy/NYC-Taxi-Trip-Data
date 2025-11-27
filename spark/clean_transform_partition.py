from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("NYC_Clean_Transform").getOrCreate()

# Read the raw Parquet dataset
df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_raw/")

# Convert timestamps
df = df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime")) \
       .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))

# Add derived columns
df = df.withColumn("pickup_date", to_date("tpep_pickup_datetime")) \
       .withColumn("pickup_hour", hour("tpep_pickup_datetime"))

# Cast numeric fields (ensures consistency)
df = df.withColumn("trip_distance", col("trip_distance").cast("double")) \
       .withColumn("fare_amount", col("fare_amount").cast("double")) \
       .withColumn("total_amount", col("total_amount").cast("double"))

# Filter out bad records
df_clean = df.filter(
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    col("pickup_date").isNotNull()
)

# Write out cleaned dataset partitioned by pickup_date
df_clean.write \
    .partitionBy("pickup_date") \
    .mode("overwrite") \
    .parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned/")

print("Finished writing cleaned + partitioned dataset.")

spark.stop()
