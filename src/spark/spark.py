import os, sys
from datetime import date, timedelta
import datetime
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import boto3, botocore
from pytz import timezone
from io import StringIO



# create spark session
conf = SparkConf()
conf.setAppName('lightning')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('lightning').getOrCreate()

# Connect to AWS S3
aws_access_key = os.environ["aws_access_key"]
aws_secret_access_key = os.environ["aws_secret_access_key"]
client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key= aws_secret_access_key)

try:
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key= aws_secret_access_key)
except Exception as e:
    print ("Can't connect to S3", e)


try:
    bucket = s3.Bucket('air-flow-lightning')
except Exception as e:
    print("Can't access air-flow-lihgtning bucket")


# Iterate the lightning files and store the processed data in dataframe  
for obj in bucket.objects.all():
    print(obj.key)
    file_name = str(obj.key)
    print("------------------------------")
    obj = client.get_object(Bucket = "air-flow-lightning", Key = obj.key)
    try:
        df = pd.read_csv(obj['Body'], error_bad_lines=False)
        print(df.describe())
    except Exception as e:
        print ("CSV file read error: ", e)
        try:
            df = pd.read_csv(obj['Body'], engine='python')
        except Exception as e:
            print ("CSV file read error: ", e)
            continue

    # Aggregate num of strikes        
    df_to_db = pd.DataFrame(columns=['Year-Month','number_of_strikes'])
    df_drop = pd.DataFrame(columns=['Year-Month','number_of_strikes'])
    df_drop['date'] = df['date']
    df_drop['number_of_strikes'] = df['number_of_strikes']       
    df_drop_group = df_drop.groupby(['date'],as_index=False)['number_of_strikes'].agg(['sum']).reset_index()
    
    # Aggregate year and month
    df_to_db['Year-Month'] = df_drop_group['date']
    df_to_db['number_of_strikes'] = df_drop_group['sum']
    df_to_db['year'] = pd.DatetimeIndex(df_to_db['Year-Month']).year
    df_to_db['month'] = pd.DatetimeIndex(df_to_db['Year-Month']).month
    df_to_db = df_to_db.groupby(['year','month'],as_index=False)['number_of_strikes'].agg(['sum']).reset_index()

    # Store aggregated data to CSV files  

    output_file_name = file_name + "output.csv"
    bucket = 'air-flow-lightning-output'
    csv_buffer = StringIO()
    df_to_db.to_csv(csv_buffer,index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket,output_file_name).put(Body=csv_buffer.getvalue())





