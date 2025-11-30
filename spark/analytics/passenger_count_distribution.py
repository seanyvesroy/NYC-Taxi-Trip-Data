from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round

def main():
    spark = SparkSession.builder.appName("PassengerCountDistribution").getOrCreate()

    df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned")
    df = df.filter(col("passenger_count") >= 1)

    total_trips = df.count()

    result = (
        df.groupBy("passenger_count")
          .agg(count("*").alias("trip_count"))
          .withColumn("percent", spark_round(col("trip_count") / total_trips * 100, 2))
          .orderBy("passenger_count")
    )

    result.show(20, truncate=False)

    # Save to HDFS
    result.write.mode("overwrite").csv(
        "hdfs://localhost:9000/data/nyc/results/passenger_count_distribution",
        header=True
    )

    # Save to local repo
    result.write.mode("overwrite").csv(
        "./results/passenger_count_distribution",
        header=True
    )

    print("\nSaved results:")
    print("  HDFS:   hdfs://localhost:9000/data/nyc/results/passenger_count_distribution")
    print("  Local:  ./results/passenger_count_distribution\n")

    spark.stop()

if __name__ == "__main__":
    main()