#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


# In[2]:


spark = SparkSession.builder \
    .appName("Covid_Data_Ingestion") \
    .master("yarn") \
    .getOrCreate()


# In[7]:


df = spark.read \
    .option("header", "true") \
    .csv("hdfs:///data/covid/raw/full_grouped.csv")


# In[13]:


df.printSchema()


# In[14]:


from pyspark.sql.functions import col

df = df.withColumn("Confirmed", col("Confirmed").cast("int"))


# In[15]:


df.printSchema()


# In[16]:


agg_df = df.groupBy("Country/Region").sum("Confirmed")

agg_df.explain("extended")


# In[ ]:


"""
Exchange
Represents shuffle operation.
Spark redistributes data across partitions using hash partitioning on Country/Region.
Required because groupBy() is a wide transformation.
Shuffle is expensive as it moves data across executors.


HashAggregate (Partial + Final)
Spark performs two-phase aggregation:
Partial aggregation before shuffle (reduces data size).
Final aggregation after shuffle.
Improves performance by minimizing shuffle data.

BroadcastHashJoin
Not present in this query.
Because no join operation is performed.

SortMergeJoin
Not present.
Appears only in large join operations.

WholeStageCodegen
Spark internally combines multiple operations (Project + HashAggregate) into optimized JVM bytecode.
Improves execution speed by reducing overhead.

"""


# In[17]:


spark.stop()


# In[ ]:




