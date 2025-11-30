from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round

def main():

    spark = SparkSession.builder \
        .appName("RevenuePerMile") \
        .getOrCreate()

    print("Loading cleaned parquet...")
    df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned")

    # Filter invalid distances (avoid divide-by-zero)
    df = df.filter(col("trip_distance") > 0)

    # Compute revenue per mile
    df = df.withColumn(
        "revenue_per_mile",
        col("total_amount") / col("trip_distance")
    )

    print("Computing average revenue per mile per hour...")

    result = (
        df.groupBy("pickup_hour")
          .agg(spark_round(avg("revenue_per_mile"), 2).alias("avg_revenue_per_mile"))
          .orderBy("pickup_hour")
    )

    result.show(24)

    # Save to HDFS
    result.write.mode("overwrite").csv(
        "hdfs://localhost:9000/data/nyc/results/revenue_per_mile",
        header=True
    )

    # Save locally for GitHub
    result.write.mode("overwrite").csv(
        "./results/revenue_per_mile",
        header=True
    )

    print("\nSaved results:")
    print("  HDFS:  hdfs://localhost:9000/data/nyc/results/revenue_per_mile")
    print("  Local: ./results/revenue_per_mile\n")

    spark.stop()

if __name__ == "__main__":
    main()
