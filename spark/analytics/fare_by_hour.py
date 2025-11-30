from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def main():
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("FareByHourAnalysis")
        .getOrCreate()
    )

    # Load cleaned + partitioned Parquet dataset
    df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned")

    # Compute average fare by pickup hour
    result = (
        df.groupBy("pickup_hour")
          .agg(avg("fare_amount").alias("avg_fare"))
          .orderBy("pickup_hour")
    )

    # Show results in terminal
    print("\n Average Fare by Pickup Hour")
    result.show(24)

        # Write to HDFS
    result.write.mode("overwrite").csv(
        "hdfs://localhost:9000/data/nyc/results/fare_by_hour",
        header=True
    )

    # Write to local repo
    result.write.mode("overwrite").csv(
        "./results/fare_by_hour",
        header=True
    )

    print("\nSaved results:")
    print("  HDFS:   hdfs://localhost:9000/data/nyc/results/fare_by_hour")
    print("  Local:  ./results/fare_by_hour\n")

    spark.stop()

if __name__ == "__main__":
    main()