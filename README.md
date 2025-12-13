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

Raw CSV (Local)  
→ HDFS (/data/nyc/raw)  
→ Spark Cleaning & Transformation  
→ Partitioned Parquet (HDFS)  
→ Analytics + Benchmarks  
→ Results (HDFS + Local CSV)  
→ Dash Dashboard

---

## Repository Structure

NYC-Taxi-Trip-Data/
├── spark/
│   ├── ingest_csv_to_parquet.py
│   ├── clean_transform_partition.py
│   ├── avg_trip_distance_by_hour.py
│   ├── fare_by_hour.py
│   ├── daily_tip_percent.py
│   ├── passenger_count_distribution.py
│   ├── revenue_per_mile.py
│   ├── trip_duration_stats.py
│   ├── benchmark_cache_effects.py
│   ├── benchmark_csv_vs_parquet.py
│   └── benchmark_shuffle_partitions.py
├── results/
│   ├── avg_trip_distance_by_hour/
│   ├── fare_by_hour/
│   ├── daily_tip_percent/
│   ├── passenger_count_distribution/
│   ├── revenue_per_mile/
│   ├── trip_duration_stats/
│   └── benchmarks/
│       ├── cache_effects/
│       ├── csv_vs_parquet/
│       └── shuffle_partitions/
├── dashboard_app.py
├── docs/
├── venv/
└── README.md

---

## Dataset

NYC Yellow Taxi Trip Records  
Source: NYC Taxi & Limousine Commission (TLC)  
Size: ~47 million rows  
Format: Raw CSV → cleaned, partitioned Parquet

---

## System Requirements

- Ubuntu 22.04+ (Ubuntu 24.04 used)
- Python 3.12
- Java 11 or 17
- Apache Hadoop (pseudo-distributed)
- Apache Spark 3.5.x
- 8–16GB RAM recommended

---

## Installation & Setup

### Install Java

sudo apt update  
sudo apt install openjdk-11-jdk

Verify:
java -version

---

### Install Hadoop

wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz  
tar -xvzf hadoop-3.3.6.tar.gz  
mv hadoop-3.3.6 ~/hadoop

Add to ~/.bashrc:

export HADOOP_HOME=$HOME/hadoop  
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin  
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

source ~/.bashrc

---

### Configure and Start HDFS

Format HDFS (first time only):

hdfs namenode -format

Start HDFS:

start-dfs.sh

Verify:

hdfs dfs -ls /

---

### Install Spark

wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz  
tar -xvzf spark-3.5.1-bin-hadoop3.tgz  
mv spark-3.5.1-bin-hadoop3 ~/spark

Add to ~/.bashrc:

export SPARK_HOME=$HOME/spark  
export PATH=$PATH:$SPARK_HOME/bin

source ~/.bashrc

---

### Python Virtual Environment

From the project root:

python3 -m venv venv  
source venv/bin/activate  

Install dependencies:

pip install --upgrade pip  
pip install pyspark==3.5.1 pandas dash plotly

---

## Running the Pipeline

### Upload Raw CSV to HDFS

hdfs dfs -mkdir -p /data/nyc/raw  
hdfs dfs -put yellow_tripdata_2016-01.csv /data/nyc/raw/

---

### Ingest CSV → Parquet

cd spark  
python ingest_csv_to_parquet.py

---

### Clean and Partition Data

python clean_transform_partition.py

Creates:

/data/nyc/parquet_clean_partitioned/

Partitioned by pickup date.

---

### Run Analytics Jobs

python avg_trip_distance_by_hour.py  
python fare_by_hour.py  
python daily_tip_percent.py  
python passenger_count_distribution.py  
python revenue_per_mile.py  
python trip_duration_stats.py  

Results are saved both locally (./results/) and in HDFS (/data/nyc/results/).

---

## Benchmarks

### Cache Effects

python benchmark_cache_effects.py

### CSV vs Parquet

python benchmark_csv_vs_parquet.py

### Shuffle Partitions

python benchmark_shuffle_partitions.py

Benchmark outputs are written to:

results/benchmarks/

---

## Dashboard

source venv/bin/activate  
python dashboard_app.py

Open in browser:

http://localhost:8050

The dashboard includes analytical results and benchmark visualizations.

---

## Authors

Sean McCarthy, Danny Yan