import sys
import config
import urllib.request
from boto.s3.key import Key
from pyspark import SparkFiles
from boto.s3.connection import S3Connection
from pyspark.sql import SparkSession, functions, types

# Covid data schema
covidSchema = types.StructType([
  types.StructField("date", types.StringType(), True),
  types.StructField("country", types.StringType(), True),
  types.StructField("state", types.StringType(), True),
  types.StructField("county", types.StringType(), True),
  types.StructField("fips", types.IntegerType(), True),
  types.StructField("lat", types.StringType(), True),
  types.StructField("long", types.StringType(), True),
  types.StructField("locationId", types.StringType(), True),
  types.StructField("actuals.cases", types.IntegerType(), True),
  types.StructField("actuals.deaths", types.IntegerType(), True),
  types.StructField("actuals.positiveTests", types.IntegerType(), True),
  types.StructField("actuals.negativeTests", types.IntegerType(), True),
  types.StructField("actuals.contactTracers", types.IntegerType(), True),
  types.StructField("actuals.hospitalBeds.capacity", types.IntegerType(), True),
  types.StructField("actuals.hospitalBeds.currentUsageTotal", types.IntegerType(), True),
  types.StructField("actuals.hospitalBeds.currentUsageCovid", types.IntegerType(), True),
  types.StructField("unused1", types.StringType(), True),
  types.StructField("actuals.icuBeds.capacity", types.IntegerType(), True),
  types.StructField("actuals.icuBeds.currentUsageTotal", types.IntegerType(), True),
  types.StructField("actuals.icuBeds.currentUsageCovid", types.IntegerType(), True),
  types.StructField("unused2", types.StringType(), True),
  types.StructField("actuals.newCases", types.IntegerType(), True),
  types.StructField("actuals.vaccinesDistributed", types.IntegerType(), True),
  types.StructField("actuals.vaccinationsInitiated", types.IntegerType(), True),
  types.StructField("actuals.vaccinationsCompleted", types.IntegerType(), True),
  types.StructField("metrics.testPositivityRatio", types.IntegerType(), True),
  types.StructField("metrics.testPositivityRatioDetails", types.StringType(), True),
  types.StructField("metrics.caseDensity", types.DoubleType(), True),
  types.StructField("metrics.contactTracerCapacityRatio", types.DoubleType(), True),
  types.StructField("metrics.infectionRate", types.DoubleType(), True),
  types.StructField("metrics.infectionRateCI90", types.DoubleType(), True),
  types.StructField("unused3", types.StringType(), True),
  types.StructField("unused4", types.StringType(), True),
  types.StructField("metrics.icuCapacityRatio", types.DoubleType(), True),
  types.StructField("riskLevels.overall", types.IntegerType(), True),
  types.StructField("metrics.vaccinationsInitiatedRatio", types.DoubleType(), True),
  types.StructField("metrics.vaccinationsCompletedRatio", types.DoubleType(), True),
  types.StructField("actuals.newDeaths", types.IntegerType(), True),
  types.StructField("actuals.vaccinesAdministered", types.IntegerType(), True),
  types.StructField("riskLevels.caseDensity", types.IntegerType(), True),
  types.StructField("cdcTransmissionLevel", types.IntegerType(), True)
])

# Function to send data to S3
def send_data_to_S3():
    covid_api_link = "https://api.covidactnow.org/v2/states.timeseries.csv?apiKey=+"config.covid_api
    response = urllib.request.urlopen(covid_api_link)
    conn = S3Connection(config.aws_key,config.aws_secret_key,host='s3.us-west-2.amazonaws.com')
    bucket = conn.get_bucket("cmpt732-project-data")
    k = Key(bucket)
    k.name = "states.timeseries.csv"
    k.set_contents_from_string(response.read(),{'Content-Type':response.info().get_content_type()})

# Fetching covid data using API call and saving to S3, and performing ETL and returning final covid_df 
def fetch_covid_data(spark,covid):
    
    # Send csv data to bucket 
    send_data_to_S3()
    
    # Fetching covid from S3 bucket and converting to dataframe
    covid_df = spark.read.csv(covid, header = True, schema = covidSchema)
    
    # selecting relevant columns
    covid_data = covid_df.select('date','country','state','fips','`actuals.cases`','`actuals.deaths`','`actuals.contactTracers`','`actuals.newCases`','`actuals.newDeaths`','`metrics.vaccinationsCompletedRatio`','`actuals.vaccinesAdministered`','`cdcTransmissionLevel`')
    
    # Creating temporary view
    covid_data.createOrReplaceTempView("covid_data")

    # Final aggregation join and 
    covid_grouped = spark.sql("""
                            SELECT YEAR(`date`) AS CovidYear, MONTH(`date`) AS CovidMonth,
                            `state`, SUM(`actuals.cases`) AS Cases, SUM(`actuals.deaths`) AS Deaths,
                            SUM(`actuals.newCases`) AS NewCases,SUM(`actuals.newDeaths`) AS NewDeaths,
                            SUM(`actuals.vaccinesAdministered`) AS VaccAdmin
                            FROM covid_data
                            GROUP BY YEAR(`date`), MONTH(`date`), `state`
    """)
    return covid_grouped