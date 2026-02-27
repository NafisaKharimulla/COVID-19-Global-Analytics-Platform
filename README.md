#  COVID-19 Global Analytics Platform  
## (Apache Spark + Local Hadoop Integration)

---

## Project Overview

This project simulates a real-world distributed data engineering environment for analyzing large-scale COVID-19 datasets.

You are working as a **Data Engineer** in a global health analytics organization, responsible for designing, implementing, optimizing, and analyzing a distributed analytics platform using:

- Apache Hadoop (HDFS + YARN, pseudo-distributed mode)
- Apache Spark (PySpark)
- Spark SQL
- RDD API
- Parquet Storage Format

All datasets are stored and processed using **HDFS**, and all Spark jobs are executed on **YARN**.

---

## System Architecture

```
Kaggle Dataset → HDFS (Raw Zone)
                 ↓
              Spark ETL
                 ↓
           Parquet (Staging)
                 ↓
        Optimized Analytics
                 ↓
        HDFS Analytics Zone
```

Execution Command:

```
spark-submit --master yarn
```

---

#  Environment Requirements

- Hadoop installed locally (HDFS + YARN running)
- Spark installed with Hadoop support
- Spark configured for YARN
- Data stored only in HDFS (No local file reads)

---

# HDFS Directory Structure

```
/data/covid/raw
/data/covid/staging
/data/covid/curated
/data/covid/analytics
```

All CSV files are uploaded to:

```
/data/covid/raw
```

---

#  Dataset Source

Kaggle Dataset:  
COVID-19 Dataset by imdevskp

Tables Used:

- full_grouped.csv
- covid_19_clean_complete.csv
- country_wise_latest.csv
- day_wise.csv
- usa_county_wise.csv
- worldometer_data.csv

---

#  Project Tasks

---

#  Task 1: Hadoop Integration

- Created structured HDFS directories
- Uploaded CSV files into `/data/covid/raw`
- Verified using HDFS CLI commands
- All Spark jobs read/write exclusively from HDFS

---

#  Task 2: Data Ingestion & Optimization

- Applied explicit schemas (No inferSchema)
- Handled null values
- Converted CSV → Parquet
- Stored optimized Parquet files in:

```
/data/covid/staging
```

###  CSV vs Parquet Comparison

Compared:
- File Size
- Read Performance
- Execution Plan
- I/O Efficiency

###  Why Parquet Performs Better

- Columnar storage
- Predicate pushdown
- Compression
- Reduced I/O
- Better query optimization

---

#  Task 3: Death Percentage Analysis

Using `full_grouped.csv`:

- Daily death percentage per country
- Global daily death percentage
- Continent-wise death percentage (joined with worldometer_data)
- Country with highest death percentage
- Top 10 countries by deaths per capita

Results written to:

```
/data/covid/analytics
```

---

#  Task 4: Infection Rate Analysis

Using `worldometer_data`:

- Confirmed cases per 1000 population
- Active cases per 1000 population
- Top 10 countries by infection rate
- WHO region infection ranking

---

# Task 5: Recovery Efficiency (Window Functions)

- Recovered percentage per country
- 7-day rolling recovery average
- Fastest recovery growth country
- Peak recovery day per country

Used Spark Window Functions.

---

#  Task 6: Global Time-Series Analysis

Using `day_wise.csv`:

- Global daily average new cases
- Spike detection using Z-score
- Global peak death date
- Month-over-Month death growth rate

---

#  Task 7: USA Drilldown Analysis

Using `usa_county_wise.csv`:

- Aggregated county data to state level
- Top 10 affected states
- Data skew detection
- Explanation of skew impact in distributed systems

---

#  Task 8: RDD-Based Implementation

Implemented using RDD API:

- Total confirmed per country
- Total deaths per country
- Death percentage using reduceByKey

###  RDD vs DataFrame Comparison

Explained:
- reduceByKey vs groupByKey
- When to avoid RDD
- Performance comparison

---

#  Task 9: Spark SQL Implementation

- Created temporary views
- SQL queries for:
  - Top 10 infection countries
  - Death percentage ranking
  - Rolling 7-day average
- Compared physical plans with DataFrame API

---

#  Task 10: Performance Optimization (Mandatory)

##  Partition Strategy

Repartitioned by:
- Date
- Country/Region

Explained:
- repartition()
- coalesce()
- Partition sizing (128–256 MB rule)

---

##  Data Skew Handling

- Identified skewed countries (e.g., USA)
- Applied salting / skew join optimization
- Explained shuffle impact

---

##  Broadcast Join

Used broadcast join for small reference datasets.

Verified using:

```
df.explain("formatted")
```

Confirmed BroadcastHashJoin usage.

---

##  Shuffle Optimization

Tuned:
- spark.sql.shuffle.partitions
- Reduced wide transformations
- Combined filters before joins

Explained why shuffle is expensive.

---

##  Caching Strategy

Used:

```
persist(StorageLevel.MEMORY_AND_DISK)
```

Explained:
- When caching improves performance
- When caching degrades performance

---

#  Task 11: Execution Plan Analysis

Used:

```
df.explain("extended")
```

Analyzed:

- Exchange
- BroadcastHashJoin
- SortMergeJoin
- WholeStageCodegen

Explained each component clearly.

---

#  Task 12: Resource & Memory Planning

Explained:

- Executor memory
- Executor cores
- YARN container allocation
- Ideal partition size (128–256 MB)
- Out-of-Memory (OOM) behavior
- Spill-to-disk mechanism

---

#  Deliverables

- Complete project folder structure
- PySpark scripts
- Spark configuration file
- HDFS command documentation
- Execution plan screenshots
- CSV vs Parquet performance comparison
- Optimization explanation document

---

#  Technologies Used

- Hadoop (HDFS + YARN)
- Apache Spark (PySpark)
- Spark SQL
- RDD API
- Parquet
- Window Functions
- Broadcast Joins
- Partitioning
- Performance Tuning

---

#  Key Learning Outcomes

- Distributed data processing
- Spark performance optimization
- Execution plan analysis
- Handling data skew
- Resource planning
- Big data architecture design

---

#  Author
Nafisa Shaik  
Data Engineer | Big Data & Distributed Systems Enthusiast  

---

This project demonstrates hands-on expertise in Hadoop + Spark distributed analytics and performance engineering.
