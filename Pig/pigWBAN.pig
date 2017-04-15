/* File: pigWBAN.pig
 Abhijeet Sarkar, 2017-04-11
 Description: Remove the unwated lines from WBAN data and prepare for loading into Hive */

/* loading data */


WBAN_Data = LOAD '/user/LZ/WBAN/wbanmasterlist.psv' USING PigStorage('|') 
AS (REGION: chararray,WBAN_ID: chararray,STATION_NAME: chararray,STATE_PROVINCE: chararray,COUNTY: chararray,COUNTRY: chararray);

WBAN_Data_No_Header = FILTER WBAN_Data BY REGION != '"REGION"';
WBAN_Data_Cleansed = FOREACH WBAN_Data_No_Header GENERATE REPLACE($0,'"',''), REPLACE($1,'"',''), REPLACE($2,'"',''), REPLACE($3,'"',''), REPLACE($4,'"',''), REPLACE($5,'"','');


/* storing in staging area  */

STORE WBAN_Data_Cleansed into '/user/STG/WBAN/Output' using PigStorage(',');


