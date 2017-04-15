/* File: pigWeather.pig
-- Abhijeet Sarkar, 2017-04-11
-- Description: Remove the unwated lines from weather data and prepare for loading into Hive 
 	Parameter: timestamp --> Passed by the calling script or terminal, represents the time at which the script is run to stamp the data accordingly*/

/* loading data */

Weather_Data = LOAD '/user/LZ/Prcp/201505hourly.txt' USING PigStorage(',')
AS (WBAN:chararray,Date:chararray,Time:int,StationType:chararray,SkyCondition:chararray,SkyConditionFlag:chararray,
Visibility:int,VisibilityFlag:chararray,WeatherType:chararray,WeatherTypeFlag:chararray,
DryBulbFarenheit:chararray,DryBulbFarenheitFlag:chararray,DryBulbCelsius:double,DryBulbCelsiusFlag:chararray,
WetBulbFarenheit:chararray,WetBulbFarenheitFlag:chararray,WetBulbCelsius:chararray,WetBulbCelsiusFlag:chararray,
DewPointFarenheit:int,DewPointFarenheitFlag:chararray,DewPointCelsius:double,DewPointCelsiusFlag:chararray,
RelativeHumidity:chararray,RelativeHumidityFlag:chararray,
WindSpeed:int,WindSpeedFlag:chararray,WindDirection:chararray,WindDirectionFlag:chararray,
ValueForWindCharacter:chararray,ValueForWindCharacterFlag:chararray,
StationPressure:chararray,StationPressureFlag:chararray,PressureTendency:chararray,PressureTendencyFlag:chararray,PressureChange:chararray,
PressureChangeFlag:chararray,
SeaLevelPressure:chararray,SeaLevelPressureFlag:chararray,RecordType:chararray,RecordTypeFlag:chararray,
HourlyPrecip:chararray,HourlyPrecipFlag:chararray,Altimeter:double,AltimeterFlag:chararray);


/* removing the header */
Weather_Data_No_Header = FILTER Weather_Data BY StationType != 'StationType';

/* keeping usable records only  */
Weather_Data_Cleansed = FOREACH Weather_Data_No_Header GENERATE WBAN, Date, Time, StationType, WindSpeed,
 WindSpeedFlag, HourlyPrecip, HourlyPrecipFlag;

/* filtering data that belongs to 12 am to 7 am */
Weather_Data_Timed = FILTER Weather_Data_Cleansed BY Time >= 701;

/* storing in staging area  */

STORE Weather_Data_Timed into '/user/STG/Prcp/Output' using PigStorage(',');
