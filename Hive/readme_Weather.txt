-- Created by Abhijeet Sarkar
-- Dated 4/14/2017
-- Objective: To join two tables based on Primary Key "WBAN"

create schema STG_WeatherTable;
use STG_WeatherTable;

-- Table 1:
create table STG_WeatherTable.Now ( WBAN string,
Date string,
Time int,
StationType string,
WindSpeed int,
WindSpeedFlag string,
HourlyPrecip string,
HourlyPrecipFlag string) row format delimited fields terminated by ',' 
STORED AS TEXTFILE;
LOAD DATA INPATH '/user/STG/Prcp/Output/part-*' INTO TABLE  STG_WeatherTable.Now; -- Note the data from source will be MOVED to Destination table directory

-- Table 2
create table STG_WeatherTable.WBAN ( REGION string,WBAN_ID string,STATION_NAME string,STATE_PROVINCE string,COUNTY string,COUNTRY string
) row format delimited fields terminated by ',' 
STORED AS TEXTFILE;
LOAD DATA INPATH '/user/STG/WBAN/Output/part-*' INTO TABLE  STG_WeatherTable.WBAN;-- Note the data from source will be MOVED to Destination table directory


-- MAP JOIN (where WBAN is the small table) and store in a HIVE Managed Table Weather_Data
CREATE TABLE Weather_Data AS 
SELECT /*+ MAPJOIN(WBAN) */ 
Now.WBAN,
Now.Date,
Now.Time,
Now.StationType,
Now.WindSpeed,
Now.WindSpeedFlag,
Now.HourlyPrecip,
Now.HourlyPrecipFlag,
WBAN.REGION,
WBAN.STATION_NAME,
WBAN.STATE_PROVINCE,
WBAN.COUNTY,
WBAN.COUNTRY 
FROM Now JOIN WBAN ON Now.WBAN = WBAN.WBAN_ID and WBAN.COUNTRY = "US" ; -- Join on Primary Key & Filtering Data not belonging to US
-- output dir: /user/hive/warehouse/stg_weathertable.db/weather_data/*
/* schema as follows:
wban                    string                                     
date                    string                                     
time                    int                                        
stationtype             string                                     
windspeed               int                                        
windspeedflag           string                                     
hourlyprecip            string                                     
hourlyprecipflag        string                                     
region                  string                                     
station_name            string                                     
state_province          string                                     
county                  string                                     
country                 string 
*/
