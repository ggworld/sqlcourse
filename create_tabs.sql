CREATE EXTERNAL TABLE IF NOT EXISTS test.dbpedia (
  `dbpedia_url` string,
  `comment_url` string,
  `wiki` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = " ",
   "quoteChar"     = "\""
)
LOCATION 's3://up-t-files/dbpedia/t1/'
TBLPROPERTIES ('has_encrypted_data'='false');
