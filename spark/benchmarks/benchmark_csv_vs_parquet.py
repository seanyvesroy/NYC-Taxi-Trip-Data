from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg
import time
import pandas as pd
import os


def timed(action_name, func):
    """Utility to time a function and print how long it took."""
    start = time.perf_counter()
    result = func()
    end = time.perf_counter()
    duration = end - start
    print(f"{action_name} took {duration:.4f} seconds")
    return result, duration


def compute_aggregation(df, hour_col_name):
    """
    Aggregation used for benchmarking.
    Expects a column 'trip_distance' and an hour column (hour_col_name).
    """
    return (
        df.groupBy(hour_col_name)
          .agg(avg("trip_distance").alias("avg_trip_distance"))
          .orderBy(hour_col_name)
    )


def main():

    spark = SparkSession.builder \
        .appName("Benchmark_CSV_vs_Parquet") \
        .getOrCreate()

    csv_path = "hdfs://localhost:9000/data/nyc/raw/yellow_tripdata_2016-01.csv"
    parquet_path = "hdfs://localhost:9000/data/nyc/parquet_clean_partitioned"

    print("\n=== Benchmark: CSV vs Parquet ===")

    # CSV Benchmark
    print("\n--- Running CSV benchmark ---")

    def run_csv():
        # Read raw CSV
        print(f"Reading CSV from {csv_path} ...")
        df_csv = (
            spark.read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv(csv_path)
        )

        # Derive pickup_hour from tpep_pickup_datetime
        df_csv = df_csv.withColumn(
            "pickup_hour",
            hour(col("tpep_pickup_datetime"))
        )

        agg_csv = compute_aggregation(df_csv, "pickup_hour")

        return agg_csv.collect()

    _, time_csv = timed("CSV (read + transform + aggregate)", run_csv)

    # Parquet Benchmark
    print("\n--- Running Parquet benchmark ---")

    def run_parquet():
        print(f"Reading Parquet from {parquet_path} ...")
        df_parquet = spark.read.parquet(parquet_path)

        agg_parquet = compute_aggregation(df_parquet, "pickup_hour")

        return agg_parquet.collect()

    _, time_parquet = timed("Parquet (read + aggregate)", run_parquet)

    # Save results
    print("\nSaving benchmark results...")

    results = pd.DataFrame([
        {"format": "csv", "seconds": time_csv},
        {"format": "parquet", "seconds": time_parquet},
    ])

    # Local directory
    local_dir = "./results/benchmarks/csv_vs_parquet"
    os.makedirs(local_dir, exist_ok=True)
    local_file = f"{local_dir}/csv_vs_parquet_benchmark.csv"
    results.to_csv(local_file, index=False)

    # HDFS directory
    hdfs_path = "hdfs://localhost:9000/data/nyc/results/benchmarks/csv_vs_parquet"
    spark.createDataFrame(results).write.mode("overwrite").csv(
        hdfs_path,
        header=True
    )

    print("\nBenchmark summary:")
    print(results)
    print("\nSaved results:")
    print(f"  Local: {local_file}")
    print(f"  HDFS:  {hdfs_path}\n")

    spark.stop()


if __name__ == "__main__":
    main()
