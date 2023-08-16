#!/bin/bash

# Author:           Rahul Goel
# Created Date:     06-05-2023
# Modified Date:    11-05-2023

# Description:      This bash file when executed will run the zomato_etl Application
# Usage:            bash wrapper.sh

# Pre-requisite:    hdfs, yarn, hive metastore and hive server2 services email configurations should be running

#importing the zomato.properties file
PROPERTIES_FILE=/home/talentum/Project/zomato_etl/script/zomato.properties

. "$PROPERTIES_FILE"

# Deleting older JSON from archive
rm -r $PROJECT_PATH/archive/*
echo "************************ Archive folder has been cleaned ****************************"

#Dropping all hive tables
echo "************************ cleaning hive *******************************************"
beeline -u jdbc:hive2://localhost:10000/default -n hiveuser -p Hive@123 -f $PROJECT_PATH/hive/ddl/cleanhive.hive --hivevar $dbname 


# Deleting current hdfs strucure and creating a new one.
bash $PROJECT_PATH/script/hdfs_st.sh
echo "*************************** HDFS structure has been created ***********************"

# Copying JSON files from staging area
bash $PROJECT_PATH/script/copy_3_json.sh
echo "************** First three JSON files has been copied from staging area **************"

copystat=$?

if [ $copystat -eq 0 ]; then


    echo "**************************** Initializing Module1 *********************************"
    bash $PROJECT_PATH/script/module_1.sh

    modstat=$?

    if [ $modstat -eq 0 ]; then

        echo "*************************Initializing Module2 *************************************"
        bash $PROJECT_PATH/script/module_2.sh

        modstat2=$?

    else 
        echo "******************** Module1 FAILED ***********************************"

        exit 1
    fi    
else 
    echo "********************** Loading of JSON files into PROJECT_PATH/source/json failed*************"

    exit 1
fi

if [ $modstat2 -eq 0 ]; then

    echo "**********************Module2 ran successfully ******************************"

    echo "**************************** Creating Backup *******************************************"
    
    #Creating Project Backup
    cp -r $PROJECT_PATH ~/shared/backup/
    echo "***********Zomato_etl has been backed up in : /shared/backup/******************"

else 
    echo "******************* Module2 FAILED, check for errors ***************************"

    exit 1
fi    


