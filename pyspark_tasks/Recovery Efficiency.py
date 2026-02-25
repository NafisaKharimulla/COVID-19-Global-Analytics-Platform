#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


# In[3]:


spark = SparkSession.builder \
    .appName("Covid_Data_Ingestion") \
    .master("yarn") \
    .getOrCreate()


# In[4]:


# Load Data
full_df = spark.read.parquet("hdfs:///data/covid/staging/full_grouped/")

full_df.show(5)


# In[5]:


# 1. Recovered percentage per country.

from pyspark.sql.functions import col, when

full_df = full_df.withColumn(
    "Confirmed",
    col("Confirmed").cast("int")
)

full_df = full_df.withColumn(
    "Recovered",
    col("Recovered").cast("int")
)

recovery_df = full_df.withColumn(
    "Recovery_Percentage",
    when(col("Confirmed") > 0,
         (col("Recovered") / col("Confirmed")) * 100
    ).otherwise(0)
)

recovery_df.select(
    "Country/Region",
    "Date",
    "Confirmed",
    "Recovered",
    "Recovery_Percentage"
).show(5)


# In[6]:


# seven day rolling Average(window Function)
from pyspark.sql.window import Window
from pyspark.sql.functions import avg

window_spec = Window.partitionBy("Country/Region") \
                    .orderBy("Date") \
                    .rowsBetween(-6, 0)

rolling_df = full_df.withColumn(
    "Rolling_7Day_Recovery_Avg",
    avg("Recovered").over(window_spec)
)

rolling_df.select(
    "Country/Region",
    "Date",
    "Recovered",
    "Rolling_7Day_Recovery_Avg"
).show(10)


# In[7]:


# Country with fastest recovery growth.

# Use lag() to get previous day
from pyspark.sql.functions import lag, col

growth_window = Window.partitionBy("Country/Region").orderBy("Date")

growth_df = full_df.withColumn(
    "Previous_Recovered",
    lag("Recovered").over(growth_window)
)

growth_df = growth_df.withColumn(
    "Recovery_Growth",
    col("Recovered") - col("Previous_Recovered")
)

# Find Maximum Growth
fastest_growth = growth_df.orderBy(
    col("Recovery_Growth").desc()
).limit(1)

fastest_growth.show()


# In[8]:


# Peak recovery day per country.

# create a window
from pyspark.sql.functions import max
peak_window = Window.partitionBy("Country/Region")

#get maximum per country
peak_df = full_df.withColumn(
    "Max_Recovered",
    max("Recovered").over(peak_window)
)

peak_day = peak_df.filter(
    col("Recovered") == col("Max_Recovered")
)

peak_day.select(
    "Country/Region",
    "Date",
    "Recovered"
).show()


# In[ ]:




