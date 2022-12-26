-- Load input file from HDFS
inputDialouges = LOAD 'hdfs:///user/umasree/inputs' USING PigStorage('\t') AS (name:chararray,line:chararray);

-- Filter out the first 2 lines
ranked = RANK inputDialouges;
ranked_lines = FILTER ranked BY (rank_inputDialouges > 2);

--Group by name
groupByName = Group ranked_lines BY name;

--Count the number of lines by each character
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
result = ORDER names BY no_of_lines DESC;

--Store results in HDFS
STORE result INTO 'hdfs:///user/umasree/output/episodeIVOutput' USING PigStorage('\t');
