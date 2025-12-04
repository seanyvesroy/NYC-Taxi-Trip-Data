from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
            .appName("CountRows")
            .getOrCreate()
    )

    df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned")

    total_rows = df.count()
    print(f"\nTotal rows in full dataset: {total_rows:,}\n")

    spark.stop()

if __name__ == "__main__":
    main()