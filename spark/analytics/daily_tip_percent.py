from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round

def main():
    spark = SparkSession.builder.appName("DailyTipPercent").getOrCreate()

    df = spark.read.parquet("hdfs://localhost:9000/data/nyc/parquet_clean_partitioned")
    df = df.filter((col("fare_amount") > 0) & (col("tip_amount") >= 0))
    df = df.withColumn("tip_percent", col("tip_amount") / col("fare_amount"))

    result = (
        df.groupBy("pickup_date")
          .agg(spark_round(avg("tip_percent") * 100, 2).alias("avg_tip_percent"))
          .orderBy("pickup_date")
    )

    result.show(10, truncate=False)

    # Save to HDFS
    result.write.mode("overwrite").csv(
        "hdfs://localhost:9000/data/nyc/results/daily_tip_percent",
        header=True
    )

    # Save to local repo
    result.write.mode("overwrite").csv(
        "./results/daily_tip_percent",
        header=True
    )

    print("\nSaved results:")
    print("  HDFS:   hdfs://localhost:9000/data/nyc/results/daily_tip_percent")
    print("  Local:  ./results/daily_tip_percent\n")

    spark.stop()

if __name__ == "__main__":
    main()
