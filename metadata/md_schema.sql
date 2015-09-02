
-- Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)
-- Author Rohan Kekatpure

-- Run this file from MySQL shell as
-- "source md_schema.sql;"

drop table if exists flowview_load_metadata;

create table flowview_load_metadata (
  topic_name varchar(500) not null ,
  database_name varchar(500) not null,
  table_name varchar(500) not null ,
  load_start_time timestamp default CURRENT_TIMESTAMP ,
  load_end_time timestamp,
  last_load_hdfs_dir varchar (500),
  last_load_hive_partition varchar (500)
);

drop table if exists flowview_load_transmitted_ratio;

create table flowview_load_transmitted_ratio (
  topic_name varchar(500) not null ,
  database_name varchar(500) not null,
  table_name varchar(500) not null ,
  load_start_time timestamp default CURRENT_TIMESTAMP ,
  load_end_time timestamp,
  hdfs_partition varchar(500) not null,
  transmitted_ratio VARCHAR (500) not null
);
