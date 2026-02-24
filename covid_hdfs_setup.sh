#!/bin/bash

echo " Creating HDFS Directory Structure "

hdfs dfs -mkdir -p /data/covid/raw
hdfs dfs -mkdir -p /data/covid/staging
hdfs dfs -mkdir -p /data/covid/curated
hdfs dfs -mkdir -p /data/covid/analytics

echo " Directory Structure Created "

echo " Uploading CSV files to RAW layer "

hdfs dfs -put -f *.csv /data/covid/raw/

echo " Files Uploaded "

echo " Verifying Directory Structure "
hdfs dfs -ls /data/covid

echo " Verifying RAW Layer Files "
hdfs dfs -ls /data/covid/raw

echo " HDFS Setup Completed Successfully "
