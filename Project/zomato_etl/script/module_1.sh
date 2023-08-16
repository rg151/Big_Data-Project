#!/bin/bash

# Author:           Rahul Goel
# Created Date:     06-05-2023
# Modified Date:    11-05-2023

# Description:      This bash file when executed will perform various functionalies of Module 1.
# Usage:            bash module_1.sh

# Pre-requisite:    hdfs, yarn, hivemetastore, hiveserver2 services should be running
#                   local directory structure should be formed
#                   JSON files present in staging area.

#importing the zomato.properties file
PROPERTIES_FILE=/home/talentum/Project/zomato_etl/script/zomato.properties

. "$PROPERTIES_FILE"

#Unsetting environment variables
source $unset_env

#Creating table zomato_summary_log
beeline -u jdbc:hive2://localhost:10000/$dbname -n hiveuser -p Hive@123 -f $PROJECT_PATH/hive/ddl/createLogTable.hive --hivevar dbname=$dbname


#Declaring StartTime
declare startTime=$(date +"%F %H:%M:%S")

#Declaring file_name to store extracted csv files from csv folders.
declare file_name=zomatocsv

#Running Spark Job
declare spark_submit_command="spark-submit --driver-java-options -Dlog4j.configuration='file:///home/talentum/spark/conf/log4j.tmp.properties' --master yarn --deploy-mode cluster $PROJECT_PATH/spark/py/module1.py" 

$spark_submit_command
declare x=$?

if [ $x -eq 0 ]; then

     echo "*******************Creating Application Instance***********************************"

     mkdir -p $PROJECT_PATH/tmp/module_1_INPROGRESS

else 
     echo "**************************Spark Job FAILED******************************************"

fi

# Extracting csv files from csv folders
count=1
for entry in $PROJECT_PATH/source/csv/$file_name/*.csv
do
	echo "${0} - entry = $entry"
	mv $entry $PROJECT_PATH/source/csv/zomato_$(date +"%Y%m%d")$count.csv
	count=$(( $count + 1 ))
done

declare status=$?

if [ $status -eq 0 ]; then

     echo "***************csv files extraction was successful, deleting file_name*********** "

     rm -rf $PROJECT_PATH/source/csv/$file_name

else

     echo "******************Extraction FAILED, csv files may not be present****************"

fi


#Declaring EndTIme
endTime=$(date +"%F %H:%M:%S")

# Creating logs

if [ $x -eq 0 ]; then

# Sending created logs to log file on local file system
     echo "$job_id,$job_step1,$spark_submit_command,$startTime,$endTime,'SUCCESS'" >> "${PROJECT_PATH}/logs/log_${log_file_name}.log"

 #    echo -e "MODULE_1 has completed execution!\nStatus:\t\t'SUCCESS'\nStart-Time:\t$startTime\nEnd-Time:\t$endTime\nFor more details, check zomato_etl/logs folder" | mail -s "module_1 Status Update: ${job_id}" aartidevikar2002@gmail.com

     echo "******************** Copying log into HDFS *********************************"

     hdfs dfs -put $PROJECT_PATH/logs/log_${log_file_name}.log /user/talentum/zomato_etl/log

     echo "******************** Loading logs to zomato_summary_log*******************"

     beeline -u jdbc:hive2://localhost:10000/default -n hiveuser -p Hive@123 -f $PROJECT_PATH/hive/dml/zomato_summary_log_dml.hive 
     
     echo "******************Removing Application instance , job was a success************"

     rm -r $PROJECT_PATH/tmp

else
     echo "**********************Job FAILED, Creating Failure logs in local file system*********"

     echo "$job_id,'module_1',$spark_submit_command,$startTime,$endTime,'FAILED'" >> "${PROJECT_PATH}/logs/log_${log_file_name}.log"
     
     echo -e "MODULE_1 has completed execution!\nStatus:\t\t'FAILED'\nStart-Time:\t$startTime\nEnd-Time:\t$endTime\nFor more details, check zomato_etl/logs folder" | mail -s "module_1 Status Update: ${job_id}" zomatoetl.group5@gmail.com     

     echo "*********************** Copying failed log into HDFS ******************************"

     hdfs dfs -put $PROJECT_PATH/logs/log_${log_file_name}.log /user/talentum/zomato_etl/log

#    Loading failed logs in zomato_summary_log
     beeline -u jdbc:hive2://localhost:10000/default -n hiveuser -p Hive@123 -f $PROJECT_PATH/hive/dml/zomato_summary_log_dml.hive
fi

echo "*********************************Moving json files into archive***********************"
mv $PROJECT_PATH/source/json/* $PROJECT_PATH/archive/


