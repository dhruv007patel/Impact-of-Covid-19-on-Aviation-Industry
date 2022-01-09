
CREATE EXTERNAL TABLE IF NOT EXISTS `cmpt_732_project`.`dest_states` (
  `dest_state` string,
  `latitude` string,
  `longitude` string,
  `dest_state_name` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cmpt732-project-data/outputfinal_output/'
TBLPROPERTIES ('has_encrypted_data'='false');
