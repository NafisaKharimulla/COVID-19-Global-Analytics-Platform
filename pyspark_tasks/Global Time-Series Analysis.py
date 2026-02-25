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


# In[6]:


#Load Data
day_df = spark.read.parquet("hdfs:///data/covid/staging/day_wise/")

day_df.show(5)
day_df.printSchema()


# In[9]:


#Global daily average new cases.

from pyspark.sql.functions import avg

avg_cases = day_df.select(
    avg("`New cases`").alias("Global_Avg_New_Cases")
)

avg_cases.show()


# In[13]:


# Global Time-Series Analysis


# calculate mean and Standard Deveation
from pyspark.sql.functions import mean, stddev, col

stats = day_df.select(
    mean("`New cases`").alias("mean_cases"),
    stddev("`New cases`").alias("std_cases")
).collect()[0]

mean_value = stats["mean_cases"]
std_value = stats["std_cases"]


# add z-scores column
z_df = day_df.withColumn(
    "Z_Score",
    (col("`New cases`") - mean_value) / std_value
)

# Detect spikes (where z >2)
spike_days = z_df.filter(col("Z_Score") > 2)

spike_days.select(
    "Date",
    "`New cases`",
    "Z_Score"
).show()


# In[14]:


# Identify Peak Death Date Globally
peak_death = day_df.orderBy(
    col("`New deaths`").desc()
).limit(1)

peak_death.select(
    "Date",
    "`New deaths`"
).show()


# In[16]:


# Month-over-Month Death Growth Rate

# Extract year and month
from pyspark.sql.functions import year, month, sum

monthly_df = day_df.withColumn(
    "Year", year("Date")
).withColumn(
    "Month", month("Date")
)

# Monthly Death totals
monthly_deaths = monthly_df.groupBy(
    "Year", "Month"
).sum("`New deaths`") \
 .orderBy("Year", "Month")

monthly_deaths.show()


# In[18]:


# Calculate Month Over Month Growth (Window Function)

from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_spec = Window.orderBy("Year", "Month")

growth_df = monthly_deaths.withColumn(
    "Previous_Month_Deaths",
    lag("sum(New deaths)").over(window_spec)
)

growth_df = growth_df.withColumn(
    "MoM_Growth_Rate",
    ((col("sum(New deaths)") - col("Previous_Month_Deaths"))
     / col("Previous_Month_Deaths")) * 100
)

growth_df.select(
    "Year",
    "Month",
    "sum(New deaths)",
    "MoM_Growth_Rate"
).show()


# In[19]:


window_spec = Window.orderBy("Year", "Month")


# In[ ]:




