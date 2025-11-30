from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg

def main():

    spark = SparkSession.builder \
        .appName("AvgTripDistanceByHour") \
        .getOrCreate()

    print("Loading cleaned parquet...")
    df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned")

    print("Computing average trip distance per hour...")

    result = (
        df.groupBy("pickup_hour")   # uses the derived column from your cleaning step
          .agg(avg("trip_distance").alias("avg_trip_distance"))
          .orderBy("pickup_hour")
    )

    result.show(24)

    # Save to HDFS
    result.write.mode("overwrite").csv(
        "hdfs://localhost:9000/data/nyc/results/avg_trip_distance_by_hour",
        header=True
    )

    # Save locally (for GitHub)
    result.write.mode("overwrite").csv(
        "./results/avg_trip_distance_by_hour",
        header=True
    )

    print("\nSaved results:")
    print("  HDFS:  hdfs://localhost:9000/data/nyc/results/avg_trip_distance_by_hour")
    print("  Local: ./results/avg_trip_distance_by_hour\n")

    spark.stop()

if __name__ == "__main__":
    main()
