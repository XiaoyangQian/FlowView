#!/bin/bash

# Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)
# Author Rohan Kekatpure

# Script to setup FlowView metadata tables in mysql database.
# This script reads sql commands from ./md_schama.sql

MD_SCHEMA_FILE="./md_schema.sql"

# Exit if schema file does not exist
if [ ! -f "$MD_SCHEMA_FILE" ]; then
    echo "Schema file does not exist. Exiting."
    exit 1
fi

echo "setting up FlowView metadata"
mysql -h pprfuedas300.corp.intuit.net \
      -u uedc_etl \
      --password=uedc@123! \
      -D uedc_metadata < "$MD_SCHEMA_FILE"
echo "done"