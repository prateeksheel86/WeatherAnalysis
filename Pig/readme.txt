1. Script: clean-population-data.pig

Description: Clean the population data for all Metropolitan State Areas (MSA). 
The data requires cleansing for quotes, and commas. 
The script filters all rows in the input file which are valid data rows. 
Then, the rows are transformed to yeild the following final result and stored in HDFS:
City, State, Population in 2000, Population in 2010, Numeric Change, Percentage Change
Input file location: /user/cloudera/LZ/Population/CPH-T-5.csv
Output file directory: /user/cloudera/STG/Population/
Command to execute: pig -x mapreduce clean-population-data.pig
