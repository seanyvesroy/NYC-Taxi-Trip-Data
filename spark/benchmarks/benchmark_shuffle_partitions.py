from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour
import time
import pandas as pd
import os

def timed(action_name, func):
    start = time.perf_counter()
    result = func()
    end = time.perf_counter()
    duration = end - start
    print(f"{action_name} took {duration:.4f} seconds")
    return result, duration

def compute_aggregation(df):
    return (
        df.groupBy("pickup_hour")
          .agg(avg("trip_distance").alias("avg_trip_distance"))
          .orderBy("pickup_hour")
    )

def main():

    spark = SparkSession.builder \
        .appName("Benchmark_Shuffle_Partitions") \
        .getOrCreate()

    parquet_path = "hdfs://localhost:9000/data/nyc/parquet_clean_partitioned"

    print("\n=== Benchmark: Shuffle Partitions ===")
    print(f"Reading cleaned parquet from {parquet_path} ...")

    df = spark.read.parquet(parquet_path)

    # Add pickup_hour column
    df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))

    total_rows = df.count()
    print(f"Dataset contains {total_rows:,} rows\n")

    shuffle_settings = [20, 50, 100, 200, 400]
    results = []

    print("\nRunning warm-up job (not recorded)...")

    def warmup():
        agg = compute_aggregation(df)
        return agg.count()

    warmup_result, warmup_time = timed("Warm-up aggregation", warmup)
    print(f"Warm-up result row count: {warmup_result}\n")

    for num_partitions in shuffle_settings:
        print(f"\n--- Benchmark with spark.sql.shuffle.partitions = {num_partitions} ---")

        spark.conf.set("spark.sql.shuffle.partitions", num_partitions)

        def run_job():
            agg = compute_aggregation(df)
            return agg.count()

        _, duration = timed(
            f"Aggregation with shuffle_partitions = {num_partitions}",
            run_job
        )

        results.append({
            "shuffle_partitions": num_partitions,
            "seconds": duration
        })

    results_df = pd.DataFrame(results)

    local_dir = "./results/benchmarks/shuffle_partitions"
    os.makedirs(local_dir, exist_ok=True)
    local_file = f"{local_dir}/shuffle_partitions_benchmark.csv"
    results_df.to_csv(local_file, index=False)

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
