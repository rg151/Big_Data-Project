#!/usr/bin/bash

mkdir -p zomato_etl/source/json 
mkdir zomato_etl/source/csv
mkdir zomato_raw_files 
mkdir zomato_etl/archive 
mkdir -p zomato_etl/hive/ddl 
mkdir zomato_etl/hive/dml 
mkdir -p zomato_etl/spark/py
mkdir zomato_etl/script 
mkdir zomato_etl/logs

cp ~/shared/zomato/* zomato_raw_files
