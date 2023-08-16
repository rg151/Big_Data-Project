#!/bin/bash

# Author:           Aarti Devikar
# Created Date:     06-05-2023
# Modified Date:    06-05-2023

# Description:      This bash file when executed will create directory structure in hdfs.
# Usage:            bash setup.sh

# Pre-requisite:    hdfs services should be running


hdfs dfs -rm -r /user/talentum/zomato_etl

echo "Creating zomato_etl"
hdfs dfs -mkdir zomato_etl

echo "Creating log and zomato_ext"
hdfs dfs -mkdir -p zomato_etl/log zomato_etl/zomato_ext

echo "Creating zomato and dim_country"
hdfs dfs -mkdir -p zomato_etl/zomato_ext/zomato zomato_etl/zomato_ext/dim_country

