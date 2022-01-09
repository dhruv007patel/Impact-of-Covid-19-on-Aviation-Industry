CREATE EXTERNAL TABLE IF NOT EXISTS `cmpt_732_project`.`covid_aviation` (
  `op_unique_carrier` string,
  `carrier_delay` float,
  `weather_delay` float,
  `nas_delay` float,
  `security_delay` float,
  `late_aircraft_delay` float,
  `arr_delay` float,
  `dep_delay` float,
  `dest_state_nm` string,
  `origin_state_nm` string,
  `year` int,
  `month` int,
  `unique_carrier_name` string,
  `passengers` float,
  `seats` float,
  `occupancy_ratio` float,
  `freight` float,
  `mail` float,
  `distance` float,
  `data_source` string,
  `carrier_origin` string,
  `dest_state_abr` string,
  `origin_state_abr` string,
  `covidyear` int,
  `covidmonth` int,
  `state` string,
  `originstatecases` float,
  `originstatedeaths` float,
  `originstatenewcases` float,
  `originstatenewdeaths` float,
  `originstatevaccadmin` float,
  `deststatecases` float,
  `deststatedeaths` float,
  `deststatenewcases` float,
  `deststatenewdeaths` float,
  `deststatevaccadmin` float
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cmpt732-project-data/outputfinal_output/'
TBLPROPERTIES ('has_encrypted_data'='false');