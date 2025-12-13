from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import time
import pandas as pd
import os

def timed(action_name, func):
    """Utility to time a function and print the result."""
    start = time.perf_counter()
    result = func()
    end = time.perf_counter()
    duration = end - start
    print(f"{action_name} took {duration:.4f} seconds")
    return result, duration


def compute_aggregation(df):
    """Aggregation used for benchmarking."""
    return (
        df.groupBy("pickup_hour")
          .agg(avg("trip_distance").alias("avg_trip_distance"))
          .orderBy("pickup_hour")
    )


def main():

    spark = SparkSession.builder \
        .appName("Benchmark_Cache_Effects") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    print("\n=== Loading cleaned parquet dataset ===")
    df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned")

    df = df.select("pickup_hour", "trip_distance").limit(2000000)


    # Without cache
    print("\nRunning WITHOUT cache...")
    def run_no_cache():
        result = compute_aggregation(df)
        return result.collect()  

    _, time_no_cache = timed("No-cache run", run_no_cache)

    # With cache
    print("\nCaching DataFrame...")
    df_cached = df.cache()

    # Trigger the cache load
    print("Materializing cache...")
    df_cached.count()

    print("\nRunning WITH cache...")
    def run_with_cache():
        result = compute_aggregation(df_cached)
        return result.collect()

    _, cache_fill_time = timed("Cache fill (count)", lambda: df_cached.count())
    _, time_cached = timed("Cached run", run_with_cache)


    # Save results
    print("\nSaving benchmark results...")

    results = pd.DataFrame([
    {"method": "no_cache", "seconds": time_no_cache},
    {"method": "cache_fill", "seconds": cache_fill_time},
    {"method": "cached_second_run", "seconds": time_cached},
])

    # Ensure local directory exists
    local_dir = "./results/benchmarks/cache_effects"
    os.makedirs(local_dir, exist_ok=True)

    results.to_csv(f"{local_dir}/cache_benchmark.csv", index=False)

    # Save to HDFS
    spark.createDataFrame(results).write.mode("overwrite").csv(
        "hdfs://localhost:9000/data/nyc/results/benchmarks/cache_effects",
        header=True
    )

    print("\nSaved results:")
    print("  Local: ./results/benchmarks/cache_effects/cache_benchmark.csv")
    print("  HDFS:  hdfs://localhost:9000/data/nyc/results/benchmarks/cache_effects\n")

    spark.stop()


if __name__ == "__main__":
    main()
