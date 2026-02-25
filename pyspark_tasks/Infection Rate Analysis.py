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


# In[4]:


from pyspark.sql.functions import col, round, when, sum

world_df = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data/")

print("worldometer_data Loaded Successfully")


# In[6]:


# 1️  Confirmed  Cases per 1000 Population

from pyspark.sql.functions import col

confirmed_df = world_df.withColumn(
    "Confirmed_per_1000",
    (col("TotalCases") / col("Population")) * 1000
)

confirmed_df.select(
    "Country/Region",
    "Confirmed_per_1000"
).show(5)


# In[9]:


# 2. Active cases per 1000 population.

from pyspark.sql.functions import col

active_df = world_df.withColumn(
    "Active_per_1000",
    (col("ActiveCases") / col("Population")) * 1000
)

active_df.select(
    "Country/Region",
    "ActiveCases",
    "Population",
    "Active_per_1000"
).show(5)


# In[10]:


# 3.Top 10 countries by infection rate.
top10_active = active_df.orderBy(
    col("Active_per_1000").desc()
).limit(10)

top10_active.show()


# In[12]:


# 4. WHO region infection ranking.

# 4.1 Group by continenet
from pyspark.sql.functions import sum, col

who_df = world_df.groupBy("Continent").sum("TotalCases", "Population")

who_df.show()


# In[13]:


# 4.2 calculate infection rate per 1000
who_df = who_df.withColumn(
    "Infection_per_1000",
    (col("sum(TotalCases)") / col("sum(Population)")) * 1000
)

who_df.select("Continent", "Infection_per_1000").show()


# In[14]:


# Rank Continents in Desc order
who_ranking = who_df.orderBy(
    col("Infection_per_1000").desc()
)

who_ranking.select("Continent", "Infection_per_1000").show()


# In[15]:


# Write to HDFS
who_ranking.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/who_region_infection_ranking/"
)


# In[ ]:




