#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


# In[5]:


spark = SparkSession.builder \
    .appName("Spark Sql Implementation") \
    .master("yarn") \
    .getOrCreate()


# In[6]:


# Load the data
world_df   = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data/")
day_df     = spark.read.parquet("hdfs:///data/covid/staging/day_wise/")
country_df = spark.read.parquet("hdfs:///data/covid/staging/country_wise_latest/")


# In[7]:


world_df.show(5)
day_df.show(5)
country_df.show(5)


# In[8]:


# Create Temporary Views
world_df.createOrReplaceTempView("world")
day_df.createOrReplaceTempView("day")
country_df.createOrReplaceTempView("country")


# In[9]:


#Top 10 Infection Countries
top10_infection = spark.sql("""
SELECT 
    `Country/Region`,
    (TotalCases / Population) * 1000 AS Infection_Per_1000
FROM world
WHERE Population > 0
ORDER BY Infection_Per_1000 DESC
LIMIT 10
""")

top10_infection.show()


# In[10]:


# Death Percentage Ranking
death_ranking = spark.sql("""
SELECT 
    `Country/Region`,
    (Deaths / Confirmed) * 100 AS Death_Percentage
FROM country
WHERE Confirmed > 0
ORDER BY Death_Percentage DESC
""")

death_ranking.show(10)


# In[11]:


# 7 Day Rolling Average
rolling_7day = spark.sql("""
SELECT
    Date,
    AVG(`New cases`) OVER (
        ORDER BY Date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS Rolling_7Day_Avg
FROM day
""")

rolling_7day.show(10)


# In[12]:


# Upload to HDFS
top10_infection.write.mode("overwrite") \
    .option("header", True) \
    .csv("hdfs:///data/covid/analytics/task9_top10_infection")

death_ranking.write.mode("overwrite") \
    .option("header", True) \
    .csv("hdfs:///data/covid/analytics/task9_death_ranking")

rolling_7day.write.mode("overwrite") \
    .option("header", True) \
    .csv("hdfs:///data/covid/analytics/task9_rolling_7day")


# In[13]:


# Compare physical plans with DataFrame API.
sql_query = spark.sql("""
SELECT 
    `Country/Region`,
    (TotalCases / Population) * 1000 AS Infection_Per_1000
FROM world
WHERE Population > 0
ORDER BY Infection_Per_1000 DESC
LIMIT 10
""")

sql_query.explain(True)


# In[14]:


sql_query = spark.sql("""
SELECT 
    `Country/Region`,
    (TotalCases / Population) * 1000 AS Infection_Per_1000
FROM world
WHERE Population > 0
ORDER BY Infection_Per_1000 DESC
LIMIT 10
""")

sql_query.explain(True)


# In[ ]:




