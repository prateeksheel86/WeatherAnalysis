1. Script: clean-population-data.pig

Description: Clean the population data for all Metropolitan State Areas (MSA). 
The data requires cleansing for quotes, and commas. 
The script filters all rows in the input file which are valid data rows. 
Then, the rows are transformed to yeild the following final result and stored in HDFS:
City, State, Population in 2000, Population in 2010, Numeric Change, Percentage Change
Input file location: /user/cloudera/LZ/Population/CPH-T-5.csv
Output file directory: /user/cloudera/STG/Population/
Command to execute: pig -f clean-population-data.pig
Optional: If the timestamp paramters is uncommented in script, use the following to pass from UNIX: --param timestamp=$(date +%s)

2. Script: pigWBAN.pig

Description: Clean the WBAN data for all Metropolitan State Areas (MSA). 
The data requires cleansing for quotes, and pipes. 
The script filters all rows in the input file which are valid data rows. 
Then, the rows are transformed to yeild the following final result and stored in HDFS:
REGION,WBAN_ID,STATION_NAME,STATE_PROVINCE,COUNTY,COUNTRY
Input file location: /user/LZ/WBAN/wbanmasterlist.psv
Output file directory: /user/cloudera/STG/Population/
Command to execute: pig pigWBAN.pig

3. Script: pigWeather.pig

Description: Clean the weather data for all Weather Stations. 
The data requires filtering of header, irrelevant data
The script filters all rows in the input file which are valid data rows. 
Then, the rows are transformed to yeild the following final result and stored in HDFS:
WBAN, Date, Time, StationType, WindSpeed,WindSpeedFlag, HourlyPrecip, HourlyPrecipFlag
Input file location: /user/LZ/Prcp/201505hourly.txt
Output file directory: /user/STG/Prcp/Output
Command to execute: pig pigWeather.pig
