## Setup API credentials
* in config.yaml stock_api section **username** and **password**
* or in airflow variables **Admin -> Variables** (app_username and app_password)

## Setup stock api connection
Create new api connection in **Admin -> Connections**; connection name **stock_api_connection** in config.yaml.
* type: HTTP
* host: https://robot-dreams-de-api.herokuapp.com

## Setup database connection
Create new postgres connection in **Admin -> Connections**; connection name **orders_pg_connection** in config.yaml.
* type: Postgres
* host: localhost
* Schema: dshop_bu
* Login: pguser
* password

## Setup hdfs connection
Create new hdfs connection in **Admin -> Connections**; connection name **hdfs_connection** in config.yaml.
* type: HDFS
* host: localhost
* port: 50070

## Setup spark connection
Create new Spark connection in **Admin -> Connections**; connection name **spark_connection** in config.yaml.
* type: Spark
* host: local

## Avalable dags (./dags):
* **stock_api** - save api data to bronze, transforms data with spark and save to silver
* **orders_db** - save db tables to bronze, transforms data with spark and save to silver. 

## Operators (./operators):
* **stock_api_to_hdfs_operator** - save stock api data to hdfs (bronze)
* **db_to_hdfs_operator** - save db tables in hdfs (bronze)
* **spark_bronze_to_silver_operator** - extract data from hdfs (bronze), make spark transformation and save to hdfs (silver)

## Transormers (./spark_transformers)
spark transformations for stock api data and db tables
