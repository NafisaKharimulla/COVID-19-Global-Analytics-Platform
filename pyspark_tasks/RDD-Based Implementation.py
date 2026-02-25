#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
spark = SparkSession.builder \
    .appName("RDD Based Implementation") \
    .master("yarn") \
    .getOrCreate()


# In[2]:


# Load the Data
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("file:///mnt/d/Covid_project/country_wise_latest.csv")

df.show(5)


# In[4]:


# convert Data Frame to RDD
rdd = df.select("Country/Region", "Confirmed", "Deaths").rdd


# In[7]:


# Total confirmed per country
confirmed_rdd = rdd.map(lambda row: (
    row["Country/Region"],
    row["Confirmed"]
))
confirmed_rdd.take(10)


# In[8]:


# Total deaths per country

deaths_rdd = rdd.map(lambda row: (
    row["Country/Region"],
    row["Deaths"]
))
deaths_rdd.take(10)


# In[9]:


# Comute death percentage using ReduceBy key
stats_rdd = rdd.map(lambda row: (
    row["Country/Region"],
    (row["Confirmed"], row["Deaths"])
))


# In[10]:


# reduce the files

reduced_stats = stats_rdd.reduceByKey(lambda x, y: (
    x[0] + y[0],
    x[1] + y[1]
))


# In[11]:


#Compute the Percentage

death_percentage = reduced_stats.map(lambda x: (
    x[0],
    x[1][0],
    x[1][1],
    (x[1][1] / x[1][0]) * 100 if x[1][0] != 0 else 0
))

death_percentage.take(10)


# In[12]:


"""
Comparison: RDD vs DataFrame Performance

1. Optimization:
   - RDD does NOT have built-in optimization.
   - DataFrame uses Catalyst Optimizer for automatic query optimization.

2. Execution Engine:
   - RDD uses low-level JVM operations.
   - DataFrame uses Tungsten execution engine (memory optimized).

3. Performance:
   - RDD is generally slower for structured data.
   - DataFrame is faster due to query planning and optimized execution.

4. Memory Efficiency:
   - RDD requires more manual handling.
   - DataFrame manages memory efficiently and reduces shuffling.

5. Ease of Use:
   - RDD requires more code and transformations.
   - DataFrame provides SQL-like operations with simpler syntax.

Conclusion:
For structured data and analytics workloads, DataFrames are preferred
because they provide better performance, optimization, and scalability.
RDD is mainly used for low-level transformations or custom processing.
"""


# In[ ]:


"""
Why reduceByKey is preferred over groupByKey
reduceByKey is preferred because it performs local aggregation before shuffle, which reduces network traffic and memory usage.
groupByKey sends all values to one executor, causing heavy shuffle and high memory consumption, making it slower.



# In[ ]:




