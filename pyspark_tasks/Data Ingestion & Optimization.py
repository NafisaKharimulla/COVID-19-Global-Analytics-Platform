#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


# In[ ]:


spark = SparkSession.builder \
    .appName("Covid_Data_Ingestion") \
    .master("yarn") \
    .getOrCreate()


# In[ ]:


#for table Day_wise.csv

from pyspark.sql.types import *

day_wise_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Confirmed", DoubleType(), True),
    StructField("Deaths", DoubleType(), True),
    StructField("Recovered", DoubleType(), True),
    StructField("Active", DoubleType(), True),
    StructField("New cases", DoubleType(), True),
    StructField("New deaths", DoubleType(), True),
    StructField("New recovered", DoubleType(), True),
    StructField("Deaths / 100 Cases", DoubleType(), True),
    StructField("Recovered / 100 Cases", DoubleType(), True)
])


# In[ ]:


df_day = spark.read \
    .option("header", True) \
    .schema(day_wise_schema) \
    .csv("hdfs:///data/covid/raw/day_wise.csv")

df_day = df_day.fillna(0)

df_day.write.mode("overwrite") \
    .parquet("hdfs:///data/covid/staging/day_wise/")

df_day.show(5)


# In[ ]:


country_schema = StructType([
    StructField("Country/Region", StringType(), True),
    StructField("Confirmed", DoubleType(), True),
    StructField("Deaths", DoubleType(), True),
    StructField("Recovered", DoubleType(), True),
    StructField("Active", DoubleType(), True),
    StructField("New cases", DoubleType(), True),
    StructField("New deaths", DoubleType(), True),
    StructField("New recovered", DoubleType(), True),
    StructField("Deaths / 100 Cases", DoubleType(), True),
    StructField("Recovered / 100 Cases", DoubleType(), True)
])


# In[ ]:


df = spark.read.option("header", True).schema(country_schema) \
    .csv("hdfs:///data/covid/raw/country_wise_latest.csv")

df.fillna(0).write.mode("overwrite") \
    .parquet("hdfs:///data/covid/staging/country_wise_latest/")


# In[ ]:


full_grouped_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Country/Region", StringType(), True),
    StructField("Confirmed", DoubleType(), True),
    StructField("Deaths", DoubleType(), True),
    StructField("Recovered", DoubleType(), True),
    StructField("Active", DoubleType(), True),
    StructField("New cases", DoubleType(), True),
    StructField("New deaths", DoubleType(), True),
    StructField("New recovered", DoubleType(), True),
    StructField("WHO Region", StringType(), True)
])


# In[ ]:


df = spark.read.option("header", True).schema(full_grouped_schema) \
    .csv("hdfs:///data/covid/raw/full_grouped.csv")

df.fillna(0).write.mode("overwrite") \
    .parquet("hdfs:///data/covid/staging/full_grouped/")


# In[ ]:


clean_schema = StructType([
    StructField("Province/State", StringType(), True),
    StructField("Country/Region", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long", DoubleType(), True),
    StructField("Date", StringType(), True),
    StructField("Confirmed", DoubleType(), True),
    StructField("Deaths", DoubleType(), True),
    StructField("Recovered", DoubleType(), True),
    StructField("Active", DoubleType(), True),
    StructField("WHO Region", StringType(), True)
])


# In[ ]:


df = spark.read.option("header", True).schema(clean_schema) \
    .csv("hdfs:///data/covid/raw/covid_19_clean_complete.csv")

df.fillna(0).write.mode("overwrite") \
    .parquet("hdfs:///data/covid/staging/covid_19_clean_complete/")


# In[ ]:


usa_schema = StructType([
    StructField("UID", StringType(), True),
    StructField("iso2", StringType(), True),
    StructField("iso3", StringType(), True),
    StructField("code3", StringType(), True),
    StructField("FIPS", StringType(), True),
    StructField("Admin2", StringType(), True),
    StructField("Province_State", StringType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long_", DoubleType(), True)
])


# In[ ]:


df = spark.read.option("header", True).schema(usa_schema) \
    .csv("hdfs:///data/covid/raw/usa_county_wise.csv")

df.fillna(0).write.mode("overwrite") \
    .parquet("hdfs:///data/covid/staging/usa_county_wise/")


# In[ ]:


world_schema = StructType([
    StructField("Country/Region", StringType(), True),
    StructField("Continent", StringType(), True),
    StructField("Population", DoubleType(), True),
    StructField("TotalCases", DoubleType(), True),
    StructField("NewCases", DoubleType(), True),
    StructField("TotalDeaths", DoubleType(), True),
    StructField("NewDeaths", DoubleType(), True),
    StructField("TotalRecovered", DoubleType(), True),
    StructField("NewRecovered", DoubleType(), True),
    StructField("ActiveCases", DoubleType(), True)
])


# In[ ]:


df = spark.read.option("header", True).schema(world_schema) \
    .csv("hdfs:///data/covid/raw/worldometer_data.csv")

df.fillna(0).write.mode("overwrite") \
    .parquet("hdfs:///data/covid/staging/worldometer_data/")


# In[ ]:


# # File Size:
# # CSV files are larger because they store data in plain text format without compression. Parquet files are smaller because they use columnar storage and built-in compression (e.g., Snappy), which reduces storage space and disk usage.

# # Read Performance:
# CSV files take more time to read because Spark must scan and parse the entire file as text. Parquet files read faster because they are optimized for analytical workloads, support column pruning, and reduce unnecessary data scanning.

# Execution Plan:
# When using CSV, the execution plan shows a full file scan since it reads row-based data. When using Parquet, Spark can apply optimizations such as column pruning and predicate pushdown, resulting in a more efficient execution plan.

# Why Parquet Performs Better:
# Parquet is a columnar storage format designed for big data analytics. It improves performance by:

# Storing data column-wise instead of row-wise

# Supporting compression to reduce file size

# Enabling column pruning (reading only required columns)

# Supporting predicate pushdown (filtering data at storage level)

# Reducing disk I/O and memory usage

# Therefore, Parquet is more efficient than CSV for large-scale analytics and distributed processing systems like Spark.


# In[ ]:




