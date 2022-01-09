import re
import sys
import json
import datetime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import ArrayType, StringType, IntegerType

# UDF function to calculate arrive delay and departure delay
get_time = udf(lambda z,y: get_time_diff(z,y),StringType())
def get_time_diff(a,b):
    a = str(a)
    a = a.rjust(4,"0")
    b = str(b)
    b = b.rjust(4,"0")
    if a == "2400":
        a = "0000"
    if b == "2400":
        b = "0000"
    t1 = datetime.datetime.strptime(a, "%H%M")
    t2 = datetime.datetime.strptime(b, "%H%M")
    
    if (a.startswith("2") and b.startswith("00")):
        t = t2-t1
        r = t.total_seconds()
        return ((r+(24*3600))/60)*-1
    elif (a.startswith("00") and b.startswith("2")):
        t = t1-t2
        r = t.total_seconds()
        return ((r+(24*3600))/60)
    else:
        t = t1-t2
        return t.total_seconds()/60
        
# UDF function to get appropriate carrier name
get_carrier_name = udf(lambda z: processed_file_name(z),StringType())
def processed_file_name(flight):
    fka_re = " fka | f/k/a "
    dba_re = " dba | d/b/a "
    if ("fka" in flight) or ("f/k/a" in flight):
        return re.split(fka_re, flight)[0]
    elif ("dba" in flight) or ("d/b/a" in flight):
        return re.split(dba_re, flight)[-1] 
    else:
        return flight 

# Fetching data from S3, performing ETL and returning data frame.
def fetch_delay_data(spark,delay,carrier):

    # Fetching the data from S3 and converting to df
    delay_df = spark.read.csv(delay,inferSchema = True, header = True).filter((col('CANCELLED')!="1") & (col('ARR_TIME').isNotNull()) & (col('DEP_TIME').isNotNull()) & (col('CRS_ARR_TIME').isNotNull()) & (col('CRS_DEP_TIME').isNotNull())).withColumn("DEP_TEMP",get_time(col("DEP_TIME"),col("CRS_DEP_TIME"))).withColumn("ARR_TEMP",get_time(col("ARR_TIME"),col("CRS_ARR_TIME")))
    
    # Fetching country_look_up
    l_country_lookup_df = spark.read.csv(carrier,inferSchema = True, header = True)

    # Dropping unnecessary columns 
    drop_columns = ["FL_DATE","ORIGIN_STATE_ABR", "DEST_STATE_ABR", "ORIGIN_STATE_FIPS","DEST_STATE_FIPS","DEP_DELAY","ARR_DELAY","ARR_DELAY_NEW","DEP_DELAY_NEW","CANCELLED","CANCELLATION_CODE","_c38"]
    delay_df = delay_df.drop(*drop_columns)

    # Joining delay data with country lookup to get full carrier names from unique_carrier_code
    delay_df = delay_df.join(l_country_lookup_df,delay_df.OP_UNIQUE_CARRIER == l_country_lookup_df.Code,"left")

    # Dropping code column to aviod data duplication after joining
    delay_df = delay_df.drop("Code").withColumnRenamed("Description","UNIQUE_CARRIER_NAME")

    # Cleaning cities name in delay data
    delay_df = delay_df.withColumn("ORIGIN_CITY_NAME",split(delay_df['ORIGIN_CITY_NAME'],',').getItem(0)).withColumn("DEST_CITY_NAME",split(delay_df['DEST_CITY_NAME'], ',').getItem(0))

    group_by_cols = ["OP_UNIQUE_CARRIER","MONTH","YEAR","DEST_STATE_NM","ORIGIN_STATE_NM","UNIQUE_CARRIER_NAME"]

    select_cols = ["OP_UNIQUE_CARRIER","UNIQUE_CARRIER_NAME","MONTH","YEAR","CARRIER_DELAY","WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY","ARR_DELAY","DEP_DELAY","DEST_STATE_NM","ORIGIN_STATE_NM"]

    # Final aggregation query to get necessary data
    delay_df = delay_df.groupBy(*group_by_cols).agg(functions.avg(delay_df['CARRIER_DELAY']).alias("CARRIER_DELAY"),functions.avg(delay_df['WEATHER_DELAY']).alias("WEATHER_DELAY"),functions.avg(delay_df['NAS_DELAY']).alias("NAS_DELAY"),functions.avg(delay_df['SECURITY_DELAY']).alias("SECURITY_DELAY"),functions.avg(delay_df['LATE_AIRCRAFT_DELAY']).alias("LATE_AIRCRAFT_DELAY"),functions.avg(delay_df['DEP_TEMP']).alias("DEP_DELAY"),functions.avg(delay_df['ARR_TEMP']).alias("ARR_DELAY")).select(*select_cols)

    # Assigning proper carrier name to final data
    delay_df = delay_df.withColumn("UNIQUE_CARRIER_NAME",get_carrier_name("UNIQUE_CARRIER_NAME"))

    return delay_df