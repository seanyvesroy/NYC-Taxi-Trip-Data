from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    min, max, avg, percentile_approx,
    unix_timestamp, col
)

def main():
    spark = SparkSession.builder \
        .appName("TripDurationStats") \
        .getOrCreate()

    print("Loading cleaned parquet...")
    df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned")

    # Compute trip duration in minutes from pickup/dropoff timestamps
    df = df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0
    )

    # Filter out weird / invalid durations
    df = df.filter(
        (col("trip_duration_minutes") > 0) &
        (col("trip_duration_minutes") < 240)  # ignore > 4 hours as outliers
    )

    print("Computing duration statistics...")

    stats = df.selectExpr(
        "min(trip_duration_minutes) AS min_trip_duration_minutes",
        "avg(trip_duration_minutes) AS mean_trip_duration_minutes",
        "percentile_approx(trip_duration_minutes, 0.5) AS median_trip_duration_minutes",
        "max(trip_duration_minutes) AS max_trip_duration_minutes"
    )

    stats.show(truncate=False)

    # Save to HDFS
    stats.write.mode("overwrite").csv(
        "hdfs://localhost:9000/data/nyc/results/trip_duration_stats",
        header=True
    )

    # Save locally (for GitHub)
    stats.write.mode("overwrite").csv(
        "./results/trip_duration_stats",
        header=True
    )

    print("\nSaved results:")
    print("  HDFS:  hdfs://localhost:9000/data/nyc/results/trip_duration_stats")
    print("  Local: ./results/trip_duration_stats\n")

    spark.stop()

if __name__ == "__main__":
    main()
