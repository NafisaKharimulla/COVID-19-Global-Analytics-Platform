#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


# In[4]:


spark = SparkSession.builder \
    .appName("Covid_Data_Ingestion") \
    .master("yarn") \
    .getOrCreate()


# In[5]:


#Read Required Tables (From STAGING Only)
full_df = spark.read.parquet("hdfs:///data/covid/staging/full_grouped/")
world_df = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data/")


# In[15]:


# 1.Compute daily death percentage per country

from pyspark.sql.functions import col, when

daily_death = full_df.withColumn(
    "Death_Percentage",
    when(col("Confirmed") != 0,
         (col("Deaths") / col("Confirmed")) * 100
    ).otherwise(0)
)
daily_death.show()


# In[17]:


from pyspark.sql.functions import col, sum, when, round

# Read full_grouped data from staging (Parquet)
full_df = spark.read.parquet("hdfs:///data/covid/staging/full_grouped/")

print("Data Loaded Successfully")


# In[18]:


# Step 1: Aggregate Global Totals Per Day

global_daily = full_df.groupBy("Date").agg(
    sum("Deaths").alias("Total_Deaths"),
    sum("Confirmed").alias("Total_Confirmed")
)

# Step 2: Calculate Death Percentage Safely

global_daily = global_daily.withColumn(
    "Global_Death_Percentage",
    round(
        when(col("Total_Confirmed") > 0,
             (col("Total_Deaths") / col("Total_Confirmed")) * 100
        ).otherwise(0),
        2
    )
)

global_daily.orderBy("Date").show(10)


# In[19]:


# Step 3: Save to Analytics Layer (HDFS)


global_daily.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/global_daily_death_percentage/"
)

print("Global Daily Death Percentage Written to HDFS")


# In[21]:


# 3.Compute continent-wise death percentage (join with worldometer_data).

# Step 1: Join on Country/Region

joined_df = full_df.join(
    world_df,
    "Country/Region",
    "inner"
)


# Step 2: Aggregate by Continent

continent_df = joined_df.groupBy("Continent").agg(
    sum("Deaths").alias("Total_Deaths"),
    sum("Confirmed").alias("Total_Confirmed")
)


# Step 3: Calculate Death Percentage Safely

continent_df = continent_df.withColumn(
    "Continent_Death_Percentage",
    round(
        when(col("Total_Confirmed") > 0,
             (col("Total_Deaths") / col("Total_Confirmed")) * 100
        ).otherwise(0),
        2
    )
)

continent_df.show()


# In[22]:


# Step 4: Save to Analytics Layer


continent_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/continent_death_percentage/"
)

print("Continent-wise Death Percentage Written to HDFS")


# In[23]:


# 4.1 Country with Highest Death Percentage


country_totals = full_df.groupBy("Country/Region").agg(
    sum("Deaths").alias("Total_Deaths"),
    sum("Confirmed").alias("Total_Confirmed")
)

country_totals = country_totals.withColumn(
    "Death_Percentage",
    round(
        when(col("Total_Confirmed") > 0,
             (col("Total_Deaths") / col("Total_Confirmed")) * 100
        ).otherwise(0),
        2
    )
)

highest_country = country_totals.orderBy(
    col("Death_Percentage").desc()
).limit(1)

highest_country.show()


# In[24]:


# 4.2 Top 10 Countries by Deaths Per Capita

per_capita = world_df.withColumn(
    "Deaths_Per_Capita",
    round(
        when(col("Population") > 0,
             col("TotalDeaths") / col("Population")
        ).otherwise(0),
        6
    )
)

top10 = per_capita.orderBy(
    col("Deaths_Per_Capita").desc()
).limit(10)

top10.show()


# Write Results to Analytics Layer

highest_country.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/highest_death_country/"
)

top10.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/top10_deaths_per_capita/"
)

print("Results Written to HDFS")


# In[ ]:




