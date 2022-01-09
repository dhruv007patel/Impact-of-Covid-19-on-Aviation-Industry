import re
import sys
import json
import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import *
import covid_data, delay_data, passengers_data
    
def main(spark, delay, carrier, passengers,covid,output):
    # Fetching covid data from S3 bucket
    covid_df = covid_data.fetch_covid_data(spark,covid)
    
    # Fetching delay data from S3 bucket
    delay_df = delay_data.fetch_delay_data(spark,delay,carrier)

    # Fetching passenger data from S3 bucket
    passengers_df = passengers_data.fetch_passengers_data(spark,passengers)

    select_cols = [delay_df.OP_UNIQUE_CARRIER,delay_df.CARRIER_DELAY,delay_df.WEATHER_DELAY, delay_df.NAS_DELAY, delay_df.SECURITY_DELAY, delay_df.LATE_AIRCRAFT_DELAY,delay_df.ARR_DELAY,delay_df.DEP_DELAY,passengers_df.DEST_STATE_NM,passengers_df.ORIGIN_STATE_NM,passengers_df.YEAR,passengers_df.MONTH,passengers_df.UNIQUE_CARRIER_NAME,passengers_df.Passengers,passengers_df.Seats,passengers_df.Occupancy_Ratio,passengers_df.Freight,passengers_df.Mail,passengers_df.Distance,passengers_df.DATA_SOURCE,passengers_df.CARRIER_ORIGIN, passengers_df.DEST_STATE_ABR, passengers_df.ORIGIN_STATE_ABR]
    
    # Joining delay_df and passengers_df for matching [year, month, origin_state, dest_state, op_unique_carrier]
    aviation_df_raw = delay_df.join(passengers_df, (delay_df.YEAR == passengers_df.YEAR) & (delay_df.MONTH == passengers_df.MONTH) & (delay_df.ORIGIN_STATE_NM == passengers_df.ORIGIN_STATE_NM) & (delay_df.DEST_STATE_NM == passengers_df.DEST_STATE_NM) & (delay_df.OP_UNIQUE_CARRIER == passengers_df.UNIQUE_CARRIER)).select(*select_cols)
    
    # Filtering relevant columns for years 2018 and 2019 from aviation_df_raw joining with covid_df
    aviation_df_18_19 = aviation_df_raw.filter((col('YEAR') == '2018') | (col('YEAR') == '2019'))\
        .withColumn('CovidYear',lit(None))\
        .withColumn('CovidMonth',lit(None))\
        .withColumn('state',lit(None))\
        .withColumn('OriginStateCases',lit(None))\
        .withColumn('OriginStateDeaths',lit(None))\
        .withColumn('OriginStateNewCases',lit(None))\
        .withColumn('OriginStateNewDeaths',lit(None))\
        .withColumn('OriginStateVaccAdmin',lit(None))\
        .withColumn('DestStateCases',lit(None))\
        .withColumn('DestStateDeaths',lit(None))\
        .withColumn('DestStateNewCases',lit(None))\
        .withColumn('DestStateNewDeaths',lit(None))\
        .withColumn('DestStateVaccAdmin',lit(None))

    # Filtering relevant columns for years 2020 and 2021 from aviation_df_raw
    aviation_df_20_21 = aviation_df_raw.filter((col('YEAR') == '2020') | (col('YEAR') == '2021'))

    # Joining covid_aviation_origin_df and aviation_df_20_21 for matching [year, month, origin_state_abr]
    covid_aviation_origin_df = aviation_df_20_21.join(covid_df, (aviation_df_20_21.YEAR == covid_df.CovidYear) & (aviation_df_20_21.MONTH == covid_df.CovidMonth) & (aviation_df_20_21.ORIGIN_STATE_ABR == covid_df.state), "left")

    # Renaming new columns to avoid data duplication
    covid_aviation_origin_df = covid_aviation_origin_df.withColumnRenamed("Cases","OriginStateCases")\
        .withColumnRenamed("Deaths","OriginStateDeaths")\
        .withColumnRenamed("NewCases","OriginStateNewCases")\
        .withColumnRenamed("NewDeaths","OriginStateNewDeaths")\
        .withColumnRenamed("VaccAdmin","OriginStateVaccAdmin")

    # Joining covid_aviation_dest_df and aviation_df_20_21 for matching [year, month, destination_state_abr]
    covid_aviation_dest_df = aviation_df_20_21.join(covid_df, (aviation_df_20_21.YEAR == covid_df.CovidYear) & (aviation_df_20_21.MONTH == covid_df.CovidMonth) & (aviation_df_20_21.DEST_STATE_ABR == covid_df.state), "left")
    
    covid_aviation_dest_df = covid_aviation_dest_df.withColumnRenamed("Cases","DestStateCases")\
        .withColumnRenamed("Deaths","DestStateDeaths")\
        .withColumnRenamed("NewCases","DestStateNewCases")\
        .withColumnRenamed("NewDeaths","DestStateNewDeaths")\
        .withColumnRenamed("VaccAdmin","DestStateVaccAdmin")

    # Create temperory views  
    covid_aviation_origin_df.createOrReplaceTempView("covid_aviation_origin_df")
    covid_aviation_dest_df.createOrReplaceTempView("covid_aviation_dest_df")

    # Final join query to concatenate all the data_frame and getting all data for year 2020 and 2021.
    join_df = spark.sql("""
                        SELECT OD.*, DD.DestStateCases, DD.DestStateDeaths,
                        DD.DestStateNewCases, DD.DestStateNewDeaths,DestStateVaccAdmin
                        FROM covid_aviation_origin_df AS OD
                        JOIN covid_aviation_dest_df AS DD
                        ON OD.OP_UNIQUE_CARRIER = DD.OP_UNIQUE_CARRIER
                        AND IFNULL(OD.CARRIER_DELAY,0) = IFNULL(DD.CARRIER_DELAY,0)
                        AND IFNULL(OD.WEATHER_DELAY,0) = IFNULL(DD.WEATHER_DELAY,0)
                        AND IFNULL(OD.NAS_DELAY,0) = IFNULL(DD.NAS_DELAY,0)
                        AND IFNULL(OD.SECURITY_DELAY,0) = IFNULL(DD.SECURITY_DELAY,0)
                        AND IFNULL(OD.LATE_AIRCRAFT_DELAY,0) = IFNULL(DD.LATE_AIRCRAFT_DELAY,0)
                        AND OD.ARR_DELAY = DD.ARR_DELAY
                        AND OD.DEP_DELAY = DD.DEP_DELAY
                        AND OD.DEST_STATE_NM = DD.DEST_STATE_NM
                        AND OD.ORIGIN_STATE_NM = DD.ORIGIN_STATE_NM
                        AND OD.YEAR = DD.YEAR
                        AND OD.MONTH = DD.MONTH
                        AND OD.UNIQUE_CARRIER_NAME = DD.UNIQUE_CARRIER_NAME
                        AND OD.Passengers = DD.Passengers
                        AND OD.Seats = DD.Seats
                        AND OD.Occupancy_Ratio = DD.Occupancy_Ratio
                        AND OD.Freight = DD.Freight
                        AND OD.Mail = DD.Mail
                        AND OD.Distance = DD.Distance
                        AND OD.DATA_SOURCE = DD.DATA_SOURCE
                        AND OD.CARRIER_ORIGIN = DD.CARRIER_ORIGIN
                        AND OD.DEST_STATE_ABR = DD.DEST_STATE_ABR
                        AND OD.ORIGIN_STATE_ABR = DD.ORIGIN_STATE_ABR
                        AND OD.CovidYear = DD.CovidYear
                        AND OD.CovidMonth = DD.CovidMonth
    """) 

    # Appending records for row-wise to make final_df having data for all 4 years. 
    final_df = aviation_df_18_19.union(join_df)
    
    # Saving the final data to S3 bucket in csv format
    final_df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save(output+"final_output")
    
if __name__ == '__main__':
    delay = sys.argv[1]        # bucket path for delay data csv: s3://cmpt732-project-data/delay/*.csv
    carrier = sys.argv[2]      # bucket path for carrier data csv: s3://cmpt732-project-data/L_UNIQUE_CARRIERS.csv
    passengers = sys.argv[3]   # bucket path for passenger data: s3://cmpt732-project-data/passengers/*.csv
    covid = sys.argv[4]        # bucket path for covid data: s3://cmpt732-project-data/states.timeseries.csv
    output = sys.argv[5]       # passing bucket path to save final output data: s3://cmpt732-project-data/output
    
    # Creating spark application
    spark = SparkSession.builder.appName('Covid_Aviation_Spark_Application').config("spark.sql.broadcastTimeout", "6000").getOrCreate()
    # main function
    main(spark, delay, carrier, passengers,covid,output)