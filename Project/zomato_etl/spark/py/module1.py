from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode
import os
import datetime as dt
import logging
import sys


# Author: Rahul Goel
# Created: 06th May 2023
# Last Modified: 10th May 2023

# Description: This pyspark application is converting json data into csv format
# Reference - Project SRS doc

# Create a Spark session
spark = SparkSession.builder.appName("json-to-csv").enableHiveSupport().getOrCreate()
sc = spark.sparkContext

log4jLogger = sc._jvm.org.apache.log4j 
logger = log4jLogger.LogManager.getLogger("module_1")

PROJECT_PATH = "/home/talentum/Project/"

#source and output paths
src_path = PROJECT_PATH + "zomato_etl/source/json/"
dest_path = "file:///" + PROJECT_PATH + "zomato_etl/source/csv/"

rm_dir = PROJECT_PATH + "zomato_etl/source/csv/"
os.system(f"rm -rf {rm_dir}")

logger.info("Reading JSON files")

try:

	for i, file in enumerate(sorted(os.listdir(src_path))):
		read_df = spark.read.json("file:///"+src_path+file)
		    
		logger.info(file+" has aquired successfully")

		logger.info(read_df.printSchema())

		order_df = read_df.select(explode(read_df.restaurants.restaurant).alias("R"))

		

		# Select the required fields
		selected_df = order_df.select(
		    col("R.R.res_ID").alias("Restaurant ID"),
		    col("R.name").alias("Restaurant Name"),
		    col("R.location.country_id").alias("Country Code"),
		    col("R.location.city").alias("City"),
		    col("R.location.address").alias("Address"),
		    col("R.location.locality").alias("Locality"),
		    col("R.location.locality_verbose").alias("Locality Verbose"),
		    col("R.location.longitude").alias("Longitude"),
		    col("R.location.latitude").alias("Latitude"),
		    col("R.cuisines").alias("Cuisines"),
		    col("R.average_cost_for_two").alias("Average Cost for two"),
		    col("R.currency").alias("Currency"),
		    col("R.has_table_booking").alias("Has Table booking"),
		    col("R.has_online_delivery").alias("Has Online delivery"),
		    col("R.is_delivering_now").alias("Is delivering now"),
		    col("R.Switch_to_order_menu").alias("Switch to order menu"),
		    col("R.price_range").alias("Price range"),
		    col("R.user_rating.aggregate_rating").alias("Aggregate rating"),
		    col("R.user_rating.rating_text").alias("Rating text"),
		    col("R.user_rating.votes").alias("Votes")
		)

		logger.info(selected_df.printSchema())
		    
		logger.info("Transformed the JSON File")


		    
		selected_df.write.mode("append").option("delimiter","\t").csv(dest_path+"zomatocsv")
		logger.info(file+" written as csv")

except:
	logger.warn("conversion of JSON to CSV has failed.")

