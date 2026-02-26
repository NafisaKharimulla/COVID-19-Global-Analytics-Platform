#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


# In[2]:


spark = SparkSession.builder \
    .appName("Performance Optimization") \
    .master("yarn") \
    .getOrCreate()


# In[4]:


# Load the Data

full_grouped_df = spark.read.parquet("hdfs:///data/covid/staging/full_grouped/")
world_df = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data/")
country_df = spark.read.parquet("hdfs:///data/covid/staging/country_wise_latest/")
full_grouped_df.printSchema()
full_grouped_df.show(5)


# In[5]:


# Partition by Date and Partition by country
df_by_date = full_grouped_df.repartition("Date")
df_by_country = full_grouped_df.repartition("Country/Region")


# In[6]:


# Apply Partitioning Before Writing Parquet
df_by_country.write \
    .mode("overwrite") \
    .partitionBy("Country/Region") \
    .parquet("hdfs:///data/covid/curated/partitioned_by_country")


# In[8]:


# Difference between repartition() and coalesce()

"""
repartition() triggers a full shuffle and is used to redistribute data evenly, while coalesce() reduces partitions
without full shuffle and is more efficient when decreasing partitions.
"""


# In[9]:


# Data Skew Handling

#Identify a skew
full_grouped_df.groupBy("Country/Region") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(5)


# In[ ]:


# based on the output i can say,There is NO data skew in this dataset.


# In[11]:


# Implement salting technique OR skew join optimization

# Create a salt Column
from pyspark.sql.functions import rand, floor, concat, lit

salted_df = full_grouped_df.withColumn(
    "salt",
    floor(rand() * 5)   # create 5 random buckets
)

# create a salted Key
from pyspark.sql.functions import col

salted_df = salted_df.withColumn(
    "salted_key",
    concat(col("Country/Region"), lit("_"), col("salt"))
)

# Use Salted key for joining
salted_join = salted_df.join(
    world_df,
    salted_df["Country/Region"] == world_df["Country/Region"]
)


# In[12]:


salted_df.select("Country/Region", "salt", "salted_key").show(10, False)


# In[14]:


df = full_grouped_df.groupBy("Country/Region").sum("Confirmed")

df.explain("formatted")


# In[ ]:


# Data Skew Handling


"""
I used explain("formatted") to inspect the physical plan and verify shuffle stages through Exchange operators.
While the dataset did not exhibit skew, the execution plan confirmed how Spark redistributes data during wide transformations.

"""


# In[15]:


#Broadcast Join

from pyspark.sql.functions import broadcast

broadcast_join_df = full_grouped_df.join(
    broadcast(world_df),
    "Country/Region"
)

broadcast_join_df.explain("formatted")


# In[ ]:


# Confirm BroadcastHashJoin is used.

"""
BroadcastHashJoin
→ Spark is using broadcast join

 BroadcastExchange
→ Small table is sent to all executors

 BuildRight
→ Right table (world_df) is broadcasted
"""


# In[16]:


# Shuffle Optimization

# Tune Shuffle Partitions
spark.conf.get("spark.sql.shuffle.partitions")



# In[20]:


# Combine filters before joins.
from pyspark.sql.functions import col

filtered = full_grouped_df.groupBy("Country/Region") \
    .sum("Confirmed") \
    .filter(col("sum(Confirmed)") > 100000)

filtered.show()


# In[ ]:


# Why Shuffle is expensive?

"""
Shuffle is expensive because it involves network data transfer, 
disk I/O, serialization overhead, and stage synchronization, making it the costliest operation in Spark.


Shuffle redistributes data across executors based on keys, causing network transfer, intermediate disk writes, and stage boundaries.
It increases I/O overhead and can introduce data skew, making it one of the most performance-critical operations in distributed processing.
"""


# In[21]:


# Caching Strategy
# Identify a DataFrame that is reused multiplet times
full_grouped_df.show()
full_grouped_df.count()
full_grouped_df.groupBy("Country/Region").count().show()


# In[22]:


# apply Caching using Persist
from pyspark import StorageLevel

full_grouped_df.persist(StorageLevel.MEMORY_AND_DISK)



# In[23]:


# Trigger an action
full_grouped_df.count()


# In[24]:


# Reuse Cached Data
full_grouped_df.show()

full_grouped_df.groupBy("Country/Region").sum("Confirmed").show()


# In[ ]:


# Why caching Degrades Performance
"""
First, I identify DataFrames that are reused multiple times. Then I apply persist(StorageLevel.MEMORY_AND_DISK) to avoid recomputation.
I trigger an action to materialize the cache. Finally, I unpersist after usage. I avoid caching very large or single-use DataFrames because 
it can increase memory pressure and degrade performance.
"""


# In[ ]:




