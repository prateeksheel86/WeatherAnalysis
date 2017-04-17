The hive commands used in this project are listed below with an associated description. For a great workflow on how to perform incremental imports into Hive tables please refer to the link given below:

https://hortonworks.com/blog/four-step-strategy-incremental-updates-hive/

1. Create database named "sample"

CREATE DATABASE IF NOT EXISTS sample WITH DBPROPERTIES('creator'='Prateek Sheel', 'date'='14-Apr-2017');

2. Create table named "population" to store the data cleansed by Pig script

CREATE EXTERNAL TABLE IF NOT EXISTS sample.population (
city STRING COMMENT 'City',
state STRING COMMENT 'State',
popApr2000 INT COMMENT 'Population in April, 2000',
popApr2010 INT COMMENT 'Population in April, 2010',
numChange INT COMMENT 'Numeric change between 2000 and 2010',
pctChange FLOAT COMMENT 'Percentage change between 2000 and 2010',
lastUpdated TIMESTAMP COMMENT 'Epoch value of when the data was ingested into HDFS') 
COMMENT 'Population EXTERNAL table containing the data for each MSA (Metropolitan State Area)'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','	
LOCATION '/user/cloudera/STG/Population'
TBLPROPERTIES ('creator'='Prateek Sheel', 'created'='2017-04-14');

3. Create managed table for projected population (partitioned by state)

CREATE TABLE IF NOT EXISTS sample.projected_population (
city STRING COMMENT 'City',
estimated_population INT COMMENT 'Projected population in May, 2015') 
COMMENT 'Projected population table containing the estimated data for each MSA (Metropolitan State Area) in May, 2015'
PARTITIONED BY (state STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('creator'='Prateek Sheel', 'created'='2017-04-14');

4. Insert data into the projected population table

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.exec.max.dynamic.partitions.pernode=1000;
INSERT OVERWRITE TABLE sample.projected_population
PARTITION(state)
SELECT UPPER(city), popApr2010 + floor(numChange*(61/120)), state
FROM sample.population;
