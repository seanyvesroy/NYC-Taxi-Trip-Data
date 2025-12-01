from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
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


def compute_aggregation(df):
    """
    Aggregation used for benchmarking.
    Expects columns: 'pickup_hour' and 'trip_distance'.
    """
    return (
        df.groupBy("pickup_hour")
          .agg(avg("trip_distance").alias("avg_trip_distance"))
          .orderBy("pickup_hour")
    )


def main():

    spark = SparkSession.builder \
        .appName("Benchmark_Shuffle_Partitions") \
        .getOrCreate()

    # ------------------------------------------------------------------
    # Paths (adjust if needed)
    # ------------------------------------------------------------------
    parquet_path = "hdfs://localhost:9000/data/nyc/parquet_clean_partitioned"

    print("\n=== Benchmark: Shuffle Partitions ===")

    # Read the cleaned Parquet once (same input for all runs)
    print(f"\nReading cleaned parquet from {parquet_path} ...")
    df = spark.read.parquet(parquet_path)

    # List of shuffle partition settings to test
    shuffle_settings = [50, 100, 200, 400]

    results = []

    # Optional warm-up run (not recorded) to "spin up" Spark
    print("\nRunning warm-up job (not recorded)...")

    def warmup():
        agg = compute_aggregation(df)
        return agg.count()  # force execution

    warmup_result, warmup_time = timed("Warm-up aggregation", warmup)
    print(f"Warm-up result row count: {warmup_result}\n")

    # ------------------------------------------------------------------
    # Run benchmark for each shuffle partition setting
    # ------------------------------------------------------------------
    for num_partitions in shuffle_settings:
        print(f"\n--- Benchmark with spark.sql.shuffle.partitions = {num_partitions} ---")

        # Set the configuration
        spark.conf.set("spark.sql.shuffle.partitions", num_partitions)

        def run_job():
            agg = compute_aggregation(df)
            # Force full execution
            return agg.count()

        _, duration = timed(
            f"Aggregation with shuffle_partitions = {num_partitions}",
            run_job
        )

        results.append({
            "shuffle_partitions": num_partitions,
            "seconds": duration
        })

    # ------------------------------------------------------------------
    # Save benchmark results (local + HDFS)
    # ------------------------------------------------------------------
    print("\nSaving benchmark results...")

    results_df = pd.DataFrame(results)

    # Local directory
    local_dir = "./results/benchmarks/shuffle_partitions"
    os.makedirs(local_dir, exist_ok=True)
    local_file = f"{local_dir}/shuffle_partitions_benchmark.csv"
    results_df.to_csv(local_file, index=False)

    # HDFS directory
    hdfs_path = "hdfs://localhost:9000/data/nyc/results/benchmarks/shuffle_partitions"
    spark.createDataFrame(results_df).write.mode("overwrite").csv(
        hdfs_path,
        header=True
    )

    print("\nBenchmark summary:")
    print(results_df)
    print("\nSaved results:")
    print(f"  Local: {local_file}")
    print(f"  HDFS:  {hdfs_path}\n")

    spark.stop()


if __name__ == "__main__":
    main()
