import os
import sys
from datetime import datetime, date, timedelta
import json

from airflow import DAG, AirflowException
from airflow.models import Variable

from config import Config
from stock_api_to_hdfs_operator import StockApiToHdfsOperator

import logging


config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "config.yaml")
app_name = "app"
cfg = Config(config_path)
app_config = cfg.app_config(app_name)

username = Variable.get("app_username", app_config["username"])
password = Variable.get("app_password", app_config["password"])

if not username or not password:
    raise AirflowException("Require api username or password in airflow variables or config.yaml")

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'retries': 2,
}

yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
yesterday = '2021-04-01'

dag = DAG(
    dag_id="export_daily_data",
    schedule_interval="@daily",
    default_args=default_args,
    start_date=datetime(2021, 12, 6, 0, 0),
)

hdfs_dir = os.path.join(app_config['directory'], yesterday)

export_api=StockApiToHdfsOperator(
    task_id="export_from_api",
    dag=dag,
    hdfs_conn_id=app_config["hdfs_connection_id"],
    http_conn_id=app_config["stock_api_connection_id"],
    endpoint=app_config["endpoint"],
    auth_endpoint=app_config["auth_endpoint"],
    date=yesterday,
    hdfs_file_path=os.path.join(hdfs_dir, "stock_api.csv"),
    username=username,
    password=password
)

export_api
