# FlowView -- Pipeline Monitoring System

## Overview
FlowView is a monitoring system that tracks data flow from data generation to HDFS to Hive.

## Usage
There are three main stages in FlowView operation: setup, load or cleanup.
'''
% python runFlowView.py --config=<ConfigFileName> --stage=setup
% python runFlowView.py --config=<ConfigFileName> --stage=load
% python runFlowView.py --config=<ConfigFileName> --stage=cleanup
'''

The setup phase sets up local file system, HDFS, and Hive table to prepare for loading job.

The load phase, ideally triggered every hour through job scheduler, fetches information through the data pipeline and writes into Hive and MySQL tables.

The cleanup phase, triggered to delete records for a dataset, cleansup local file system, HDFS, and records of the particular dataset in MySQL.

## Design
Along the pipeline, FlowView tracks and registers message-level metadata details by tapping into each transition point, fetching message timestamps, and registers them into a Hive table. 

With each dataset onboarded, two hive tables are generated:
<ul>
	<li>HDFS_timestamp, containing timestamps from data generation (server) and arriving at HDFS</li>
	<li>Hive_timestamp, containing timestamps from data being parsed out from JSON format and written into Hive warehouse</li>
</ul>

Additionally, mySQL database is used to keep track of FlowView metadata information. Two tables are maintained and used for all datasets:
<ul>
	<li>flowview_load_metadata, to keep track of last processed HDFS and Hive file</li>
	<li>flowview_load_transmission_ratio, to keep track of the percentage of data loss per load for each dataset</li>
</ul>

## Visualization / Reporting
FlowView is intended to be the middle layer between the data transmission pipeline and visualization tools such as Grafana and Kibana. The users are welcomed to do further downstream processing through the Hive tables, generate the metrics of their concern, and ingest the output into the reporting tools of their choice. 

