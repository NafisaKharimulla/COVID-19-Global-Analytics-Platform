#!/usr/bin/env python
# coding: utf-8

# In[9]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


# In[10]:


spark = SparkSession.builder \
    .appName("USA Drilldown Analysis") \
    .master("yarn") \
    .getOrCreate()


# In[12]:


# load the data
usa_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("file:///mnt/d/Covid_project/usa_county_wise.csv")

usa_df.show(5)
usa_df.printSchema()


# In[15]:


# Aggregate county data to state level.

from pyspark.sql.functions import sum

state_df = usa_df.groupBy("Province_State").agg(
    sum("Confirmed").alias("Total_Confirmed"),
    sum("Deaths").alias("Total_Deaths")
)

state_df.show()


# In[17]:


# upload to hdfs
state_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs:///covid/output/state_level_aggregation")


# In[18]:


# Identify top 10 affected states
from pyspark.sql.functions import desc

top10_states = state_df.orderBy(desc("Total_Confirmed")).limit(10)

top10_states.show()


# In[19]:


top10_states.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs:///covid/output/top10_affected_states")


# In[20]:


# Detect data skews across countries

# count records per state
from pyspark.sql.functions import desc

state_distribution = usa_df.groupBy("Province_State") \
    .count() \
    .orderBy(desc("count"))

state_distribution.show()


# In[21]:


# check max vs min record count
from pyspark.sql.functions import max, min

state_distribution.select(
    max("count").alias("Max_Record_Count"),
    min("count").alias("Min_Record_Count")
).show()


# In[22]:


state_distribution.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs:///covid/output/state_record_distribution")


# In[23]:


# TASK 7.4 – Explain Data Skew Impact in Distributed Systems

"""
Data Skew in Distributed Systems:

Data skew occurs when certain keys (e.g., specific states like California)
have significantly more records than others.

Impact in Spark/Hadoop:

1. Uneven Partition Sizes:
   Some partitions become very large while others are small.

2. Executor Imbalance:
   One executor processes a huge partition while others finish early.

3. Increased Job Completion Time:
   Spark waits for the slowest task to complete.

4. Resource Underutilization:
   Some nodes remain idle while one node is overloaded.

5. Memory Pressure:
   Large partitions may cause OutOfMemory errors or disk spilling.

6. Shuffle Bottlenecks:
   Skewed keys during groupBy or join operations cause heavy data transfer
   to a single partition, increasing network overhead.

Conclusion:
Data skew reduces parallel efficiency and increases execution time
in distributed systems.
"""


# In[ ]:




