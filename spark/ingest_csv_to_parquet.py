from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NYC_Ingest_CSV_to_Parquet") \
    .getOrCreate()

# Read all CSVs in raw folder
df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/nyc/raw/")

print("Schema:")
df.printSchema()

print("Sample rows:")
df.show(10)

# Write to Parquet (unpartitioned for now)
df.write \
    .mode("overwrite") \
    .parquet("hdfs:///data/nyc/curated/parquet/")

print("Finished writing parquet!")

spark.stop()
