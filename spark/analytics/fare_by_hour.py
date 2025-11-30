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
    fare_by_hour = (
        df.groupBy("pickup_hour")
          .agg(avg("fare_amount").alias("avg_fare"))
          .orderBy("pickup_hour")
    )

    # Show results in terminal
    print("\n Average Fare by Pickup Hour")
    fare_by_hour.show(24)

    # Save results for plotting (optional)
    output_path = "hdfs://localhost:9000/data/nyc/results/fare_by_hour"
    fare_by_hour.write.mode("overwrite").csv(output_path, header=True)

    print(f"\nResults saved to: {output_path}\n")

    spark.stop()

if __name__ == "__main__":
    main()