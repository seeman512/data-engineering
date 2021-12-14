## Setup API credentials
* in dags/config.yaml **username** and **password**
* or in airflow variables **Admin -> Variables** (app_username and app_password)

## Setup stock api connection
Create new api connection in **Admin -> Connections**; connection name **dz_5_stock_api_connection** in dags/config.yaml.
* type: HTTP
* host: https://robot-dreams-de-api.herokuapp.com

## Setup database connection
Create new postgres connection in **Admin -> Connections**; connection name **dz5_pg_connection** in dags/config.yaml.
* type: Postgres
* host: localhost
* Schema: dshop_bu
* Login: pguser
* password

## Setup hdfs connection
Create new hdfs connection in **Admin -> Connections**; connection name **dz5_hdfs_connection** in dags/config.yaml.
* type: HDFS
* host: localhost
* port: 50070

## Avalable dags:
* **export_initial_data** - exports all database tables
* **export_daily_data** - exports yesterday's databse orders and API products
