# Duo-flow


## Introduction

This program is developed by Qing Guo as Insight Data Science project in Jun, 2020. 

Apache Airflow is a platform to programmatically author, schedule and monitor workflows.Airflow is used to author workflows as Directed Acyclic Graphs (DAGs) of tasks(https://airflow.apache.org/docs/stable/). The Airflow scheduler executes tasks on an array of workers while following the specified dependencies. Airflow is typically deployed on one machine but for high-volume jobs, it may make sense to have multiple machines running different workflows. One machine is designed to act as the scheduler when multiple Airflow jobs are running. However if that machine goes down, one may need to run the entire workflow again instead of picking up where it left off. Here an alternative is proposed.

## Solution

The approach is to scale out the Airflow cluster. A single node Airflow architecture contains webserver, scheduler, executor, metastore database and queueing system. In Airflow cluster, all these components can be deployed on different machines. In this way, we can add a spare scheduler as a standby scheduler. The standby scheduler and the main scheduler share the same metadate which is stored in an external database. Instead of one scheduler, two schedulers are running and synchronizing using a heartbeat. If the first one goes down, the second one will take over the job immediately. 

![alt text](https://github.com/CottenNoTail/Duo-flow/blob/master/Images/airflow%20archietecture.png)


## How to use this program
To create an infrastructure like this we need to do the following steps:

1. Install & Configure Airflow with RabbitMQ and Celery Executor support – on schedulers and workers

`sudo pip install apache-airflow[celery,rabbitmq,s3,postgres,crypto,jdbc]`
`sudo pip install psycopg2`

2. Install & Configure RabbitMQ on a separate host 

`sudo apt-get install erlang`
`sudo apt-get install rabbitmq-server`

3. Install Postgresql on a separate host

sudo apt-get install python-psycopg2
sudo apt-get install postgresql postgresql-contrib

4. Start airflow workers

`airflow worker`

5. Run a DAG



The fault tolerant Duo-flow need to be installed on two schedulers. It can be called using

`sh run.sh`

The python script can also be called using:

`python 3.7 ./src/main.py`


## Data Description

The dataset contains cloud-to-ground lightning strike information collected by Vaisala's National Lightning Detection Network and aggregated into 1.1 degree tiles by the experts at the National Centers for Environmental Information (NCEI) as part of their Severe Weather Data Inventory. This data provides historical cloud-to-ground data aggregated into tiles that around roughly 11 KMs for redistribution. This provides users with the number of lightning strikes each day, as well as the center point for each tile. The sample queries below will help you get started using BigQuery's GIS capabilities to analyze the data. 

Dataset source: NOAA

https://console.cloud.google.com/marketplace/details/noaa-public/lightning

## Data pipeline

A fully functional data pipeline is constructed to test the fault tolerant Airflow. The data pipeline is as follows:

'NOAA lightning Dataset' in BigQuery -> Google Cloud Storage -> AWS S3 -> Spark Batch processing -> PostgreSQL -> Dash Plotly

![alt text](https://github.com/CottenNoTail/Duo-flow/blob/master/Images/workflow.png)

## Repo directory structure
The top-level directory structure look like the following: 

    ├── README.md
    ├── run.sh
    ├── app
         └── app.py
    ├── src
        └── dags
            └── dags.py
        └── spark
            └──  spark.py
        └── database
                ├── db.py
                ├── config.py 
        └── main.py
 
## Details about the script
A table is inserted in the metadata file in Postgresql. This table has two rows to store `Last_heartbeat` and `scheduler`. `Last_heartbeat` is used to compare with the current time. If the interval exceed some theshold, the standby scheduler will take over the job. `scheduler` is used to indicate which scheduler is running right now.

Finally, I would like to thank Yagizkaymak for the pioneer work done before this project!





