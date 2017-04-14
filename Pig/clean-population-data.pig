-- File: clean-population-data.pig
-- Prateek Sheel, 2017-04-11
-- Description: Remove the unwated lines from population data and prepare for loading into Hive

%declare time '$timestamp ';

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
datawithtimestamp = foreach finaldata generate $0, $1, $2, $3, $4, $5, '$time';
STORE datawithtimestamp into '/user/cloudera/STG/Population' using PigStorage(',');
--dump finalspecialdata;
