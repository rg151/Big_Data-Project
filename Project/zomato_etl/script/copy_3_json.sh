#!/bin/bash

# Author:           Aarti Devikar
# Created Date:     06-05-2023
# Modified Date:    10-05-2023

# Description:      This bash file when executed will copy JSON file from staging area to source/json/.
# Usage:            bash copy_3_json.sh

# Pre-requisite:    JSON files should be present in staging area

cp ~/Project/zomato_raw_files/file{1..3}.json ~/Project/zomato_etl/source/json
