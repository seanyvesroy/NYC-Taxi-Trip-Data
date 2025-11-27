from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyCleanedNYC").getOrCreate()

df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned/")

print("\nSchema:")
df.printSchema()

print("\nSample cleaned rows:")
df.show(10)

print("\nTotal rows in cleaned dataset:")
print(df.count())

print("\nUnique pickup_date values:")
df.select("pickup_date").distinct().orderBy("pickup_date").show(20)

spark.stop()