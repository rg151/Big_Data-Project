#!/bin/bash

# Author:           Rahul Goel
# Created Date:     06-05-2023
# Modified Date:    11-05-2023

# Description:      This bash file when executed will perform various functionalies of Module 2.
# Usage:            bash module_2.sh

# Pre-requisite:    hdfs, yarn, hivemetastore,hiveserver services should be running
#					module_1.sh should have run successfully.

#importing the zomato.properties file

. "/home/talentum/Project/zomato_etl/script/zomato.properties"

declare spark_submit_command2="NA"

#Declaring Module 2 start time
declare startTime2=$(date +"%F %H:%M:%S")

dbname=default

echo "----------------------- module_2.sh started----------------------------------"

if [ -d /home/talentum/Project/zomato_etl/tmp ]; then

	echo "---------------------Another instance is already running---------------------"

else
    echo "--------------------Creating instance for module2--------------------------"
	mkdir -p $PROJECT_PATH/tmp/module_2_INPROGRESS

fi

echo "----------------------Job Start Time is $startTime2-------------------------"
echo "----------------------Hive DB Name is $dbname----------------------------------"

#Creating table dim_country
beeline -u jdbc:hive2://localhost:10000/default -n hiveuser -p Hive@123 -f $PROJECT_PATH/hive/ddl/createCountry.hive --hivevar dbname=$dbname 
echo "--------------------- Hive dim_country table created-----------------------------"

#Creating tables raw_zomato and zomato
beeline -u jdbc:hive2://localhost:10000/default -n hiveuser -p Hive@123 -f $PROJECT_PATH/hive/ddl/createZomato.hive --hivevar dbname=$dbname
echo "-------------------Hive raw_zomato and zomato tables created---------------------------"

#Loading dim_country
beeline -u jdbc:hive2://localhost:10000/default -n hiveuser -p Hive@123 -f $PROJECT_PATH/hive/dml/loadIntoCountry.hive --hivevar dbname=$dbname
echo "---------------------Hive dim_country table populated----------------------------"

#Loading raw_zomato
beeline -u jdbc:hive2://localhost:10000/default -n hiveuser -p Hive@123 -f $PROJECT_PATH/hive/dml/loadIntoRaw.hive --hivevar dbname=$dbname
echo "---------------------Hive raw_zomato table populated-------------------------------"

#Loading zomato
beeline -u jdbc:hive2://localhost:10000/default -n hiveuser -p Hive@123 -f $PROJECT_PATH/hive/dml/loadIntoZomato.hive --hivevar dbname=$dbname
echo "-----------------------Hive zomato table populated------------------------------"




#Preparing log message
STATUS=$?
if [ $STATUS == "0" ]; then
	STATUS="SUCCEEDED"
	echo "------------------------------STATUS = $STATUS-------------------------------"
	
else
	STATUS="FAILED"
	echo "${0} - STATUS = $STATUS"
	echo "Failed in module 2" | mail -s "Module2" zomatoetl.group5@gmail.com
fi

endTime2=$(date +"%F %H:%M:%S")
echo "----------------------Job End Time is $endTime2-----------------------------"

# Adding log message into a log file on local file system
timestamp=$(date +%Y)
echo "$job_id2,$job_step2,$spark_submit_command2,$startTime2,$endTime2,$STATUS" >> "${PROJECT_PATH}/logs/log_${log_file_name}.log"


# Loading the log file from local file system to Hive table
# Create if not exists temporary/managed table default.zomato_summary_log with location clause /user/talentum/zomato_etl/log

beeline -u jdbc:hive2://localhost:10000/$dbname -n hiveuser -p Hive@123 --hivevar dbname=$dbname -f $PROJECT_PATH/hive/ddl/createLogTable.hive --hivevar dbname=$dbname
echo "------------------Hive zomato_summary_log table created if not exists-----------"

# Load logfile into table default.zomato_summary_log

echo "--------------------Copying log into HDFS --------------------------------------" 
hdfs dfs -put $PROJECT_PATH/logs/log_${log_file_name}.log /user/talentum/zomato_etl/log

#Loading zomato_summary_log
beeline -u jdbc:hive2://localhost:10000/$dbname -n hiveuser -p Hive@123 --hivevar dbname=$dbname -f $PROJECT_PATH/hive/dml/zomato_summary_log_dml.hive --hivevar dbname=$dbname
echo "-----------------------Hive zomato_summary_log table populated---------------------"

# deleting module 2 instance
rm -r $PROJECT_PATH/tmp

echo "--------------------------removal of the instance of module 2 done------------------"

echo "-------------------------------module_2.sh ended--------------------------------------"








