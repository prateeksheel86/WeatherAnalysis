-- File: clean-population-data.pig
-- Prateek Sheel, 2017-04-11
-- Description: Remove the unwated lines from population data and prepare for loading into Hive
-- Parameter: timestamp --> Passed by the calling script or terminal, represents the time at which the script is run to stamp the data accordingly

--%declare time '$timestamp'; -- Not used in this script. But, it could be used in line 26 to generate the timestamp

population = LOAD '/user/cloudera/LZ/Population/CPH-T-5.csv' USING PigStorage() AS (linedata: chararray); -- Load file
filtered = filter population by linedata matches '^["]\\p{L}+(?:[- ]\\n?\\p{L}+)*[,]\\s[A-Z]{2}["].*';  -- Extract all records that match the MSA pattern. e.g. "Abilene, TX"

quotesplit = foreach filtered generate FLATTEN(STRSPLIT(linedata, '"')); -- Split the string based on double quotes
specialdata = filter quotesplit by $6 != ','; -- Some of the rows have quotes for the numeric change, will be handeled separately
regulardata = filter quotesplit by $6 == ','; -- Other rows do not have quotes for the numeric change

-- Process regular data
cleanregdata = foreach regulardata generate FLATTEN(STRSPLIT($1, ',')), REPLACE($3, ',', ''), REPLACE($5, ',', ''), REPLACE($7, ',', ''), REPLACE($8, ',', '');
finalregdata = foreach cleanregdata generate $0, TRIM($1), $2, $3, $4, $5;

-- Process special data
cleanspecialdata = foreach specialdata generate FLATTEN(STRSPLIT($1, ',')), REPLACE($3, ',', ''), REPLACE($5, ',', ''), FLATTEN(STRSPLIT($6, ','));
finalspecialdata = foreach cleanspecialdata generate $0, TRIM($1), $2, $3, $5, $6;

-- Join both data sets
finaldata = union finalregdata, finalspecialdata;
datawithtimestamp = foreach finaldata generate $0, $1, $2, $3, $4, $5, ToUnixTime(CurrentTime());
STORE datawithtimestamp into '/user/cloudera/STG/Population' USING PigStorage(',');

-- Extra: Usage of time parameter mentioned in line 6 above
-- datawithtimestamp = foreach finaldata generate $0, $1, $2, $3, $4, $5, '$time'; -- Use of the "time" variable declared above

-- Extra: Storing data in Parquet format
-- datawithtimestamp = foreach finaldata generate $0 AS city: chararray, $1 AS state: chararray, $2 AS popApril2000: chararray, $3 AS popApril2010: chararray, $4 AS numChange: chararray, $5 AS pctChange: chararray, ToUnixTime(CurrentTime()) AS timestamp: long; -- Specify schema so that the Parquet storer can use it while writing the file
-- STORE datawithtimestamp into '/user/cloudera/STG/Population' USING parquet.pig.ParquetStorer;
