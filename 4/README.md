## Setup API credentials
* in dags/config.yaml **username** and **password**
* or in airflow variables **Admin -> Variables** (app_username and app_password)

## Setup database connection
Create new postgres connection in **Admin -> Connections**; connection name **dz4_pg_connection** in dags/config.yaml.

## Avalable dags:
* **export_initial_data** - exports all database tables
* **export_daily_data** - exports yesterday's databse orders and API products
