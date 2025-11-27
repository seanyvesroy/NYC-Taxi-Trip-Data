# NYC Taxi Trip Data — Apache Spark + Hadoop Analytics Project

This project implements a complete big data pipeline using **Apache Spark**, **HDFS**, and **Python** to process and analyze NYC Yellow Taxi trips.

## Features

### Data Engineering Pipeline
- CSV → Parquet conversion using Spark
- Cleaning + timestamp parsing
- Partitioning by pickup_date
- Writing optimized Parquet datasets to HDFS

### Analytics
- Average fare by hour
- Top pickup zones
- Revenue per mile
- Trip distance/fare correlations

### Performance Benchmarks
- CSV vs Parquet performance comparison
- Cached vs non-cached Spark jobs
- Different shuffle partition settings

## Architecture Overview
