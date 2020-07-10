import sys, os, boto3
import datetime as dt
import datetime
import psycopg2
import pandas as pd
import fnmatch, re
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.sensors import S3KeySensor
from airflow.models import Variable
import airflow.operators
import boto3, botocore
from pytz import timezone


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 6, 10, 00, 00, 00),
    'email': ['ching.guonk@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=1)
}

dag = DAG('Lightning_DAG', default_args=default_args, schedule_interval='@daily')

def conn_db():
    """ Returns database connection object """
    try:
        self.conn = psycopg2.connect(
        host=os.environ.get('HOST'),
        database=os.environ.get('database'),
        user=os.environ.get('USER'),
        password=os.environ.get('PSWD'))

        conn_string = 'postgresql://' + str(user) + ':' + str(password) + str(host)
        engine = create_engine(conn_string)
        print("connect to PG")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return engine

def store_db(**kwargs):
    """ Read results stored in AWS S3 and store it to Postgres """
   
    engine = conn_db()
    
    # create access to AWS and airflow lightning Bucket """    
    aws_access_key = os.environ["aws_access_key"]
    aws_secret_access_key = os.environ["aws_secret_access_key"]
    client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key= aws_secret_access_key) 
    try:
        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key= aws_secret_access_key)
    except Exception as e:
        print ("Boto3 connection error: ", e)
    try:
        bucket = s3.Bucket('air-flow-lightning-output')
    except Exception as e:
        print("can't access air-flow-lightning-output bucket!")
        
        
    # Read CSV files and store the result to database
    for obj in bucket.objects.all():
        print(obj.key)
        file_name = str(obj.key)
        print("------------------------------")
        obj = client.get_object(Bucket = "air-flow-lightning-output", Key = file_name)
        try:
            df = pd.read_csv(obj['Body'], error_bad_lines=False)
        except Exception as e:
            print ("CSV file read error: ", e)
            continue
        try:
            df.to_sql('lightning', con=engine, index=False, if_exists='replace')
        except (IOError, psycopg2.Error) as error:
            print ("Error inserting dataframe to DB!", error)


# Bash operator that synchronizes the NOAA Dataset with bucket stored in S3
s3_ingest = BashOperator(
    task_id='s3_ingest', 
    bash_command='aws s3 sync s3://air-flow-lightning s3://airflow-lightning-origin', 
    queue='default',
    dag=dag)

# Spark processing operator 

spark_batch = BashOperator(
    task_id='spark_batch', 
    bash_command='spark-submit ~/code/Duo-flow/spark.py',
    queue='default', 
    dag=dag)

# S3 file sensor operator which senses the newly creatly file in S3
s3_file_sensor = S3KeySensor(
    task_id='s3_file_sensor',
    queue='default'
    bucket_key='s3://air-flow-lightning-output/lightning_2020output.csv',
    bucket_name=None,
    queue='default',
    dag=dag)

# Store to DB operator that stores the result in PostgreSQL
store_db = PythonOperator(
    task_id = 'store_db', 
    provide_context=True,
    python_callable=store_db, 
    queue='default',
    dag=dag)


# Create dependencies for the DAG
s3_ingest >> spark_batch>> s3_file_sensor>> store_db
