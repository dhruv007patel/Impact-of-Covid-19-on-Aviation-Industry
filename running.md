# Installing
## _Setting up AWS EMR cluster_

- Create an Amazon AWS account.
- Create a S3 Bucket where the required data will be stored. The data can be uploaded to the delay directory and passenger directory from [one] and [two]
- Set up AWS CLI in your local system and edit the credentials in ~/.aws/credentials file.
- While creating the cluster select the following Software Cofigurations
-- Release emr-6.5.0
-- Applications Spark environment - Spark 3.1.2 on Hadoop 3.2.1 YARN with and Zeppelin 0.10.0
- While creating the cluster select the following Hardware Configurations
-- Select one master node (m5.xlarge or preferred) and two worker nodes (m4.large or preferred)

Upload the below mentioned files in S3
- covid_data.py
- delay_data.py
- passenger_data.py
- main.py
- datasets (passenger and delay data)
## _Submitting the Spark Application_

- In the add steps dialogue enter the following fields
-- Step type (Dropdown): Spark Application
-- Name (Text): Relative average Reddit-1
-- Deploy mode (Dropdown): Client
-- Spark-submit options (Text): --conf spark.yarn.maxAppAttempts=1 --py-files s3://cmpt732-project-data/config.py,s3://cmpt732-project-data/covid_data.py,s3://cmpt732-project-data/passengers_data.py,s3://cmpt732-project-data/delay_data.py 
-- Application location (Text): s3://cmpt732-project-data/main.py 
-- Arguments (Text): s3://cmpt732-project-data/delay/*.csv s3://cmpt732-project-data/L_UNIQUE_CARRIERS.csv s3://cmpt732-project-data/passengers/*.csv  s3://cmpt732-project-data/states.timeseries.csv  s3://cmpt732-project-data/output
-- Action on failure (Dropdown): Continue [Default]
- Click on "Add" button
- After successful execution, a csv file will be created on S3 inside "outputfinal_output" folder. 
- Further visualisation is done on Tableau using AWS-Athena. 



[//]: #

   [one]: <https://1sfu-my.sharepoint.com/:f:/g/personal/krt4_sfu_ca/EsQhv_uPoFhMnibqiD4s29EBkbo7h_cXaS84qL34N7Chhg?e=IMTE3n/>
   [two]: <https://1sfu-my.sharepoint.com/:f:/g/personal/krt4_sfu_ca/EkTjMdv6AZdJiTE03igJ_K0BgvISiD6hHqUp-UFjP7nS3A?e=jCvyVB>