import os
import sys
from datetime import datetime, date, timedelta
import logging

from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook

from pyspark.sql import SparkSession

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from operators.stock_api_to_hdfs_operator import StockApiToHdfsOperator
from operators.spark_bronze_to_silver_operator import SparkBronzeToSilverOperator
from config import Config


config_path = os.path.join(parent_dir, "config.yaml")
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

spark_conn = BaseHook.get_connection(app_config["spark_connection_id"])
spark = SparkSession.builder\
    .master(spark_conn.host)\
    .appName("stock_api_dag")\
    .getOrCreate()

dag = DAG(
    dag_id="stock_api",
    schedule_interval="@daily",
    default_args=default_args,
    start_date=datetime(2021, 12, 6, 0, 0),
)

start_task = DummyOperator(
    task_id='start_stock_api',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_stock_api',
    dag=dag,
)

bronze_file_path = os.path.join(app_config["bronze_directory"],
                                "stock_api",
                                yesterday,
                                "products.json")
silver_file_path = os.path.join(app_config["silver_directory"],
                                "stock_api_products")

to_bronze_task = StockApiToHdfsOperator(
    task_id="from_stock_api_to_bronze",
    dag=dag,
    hdfs_conn_id=app_config["hdfs_connection_id"],
    http_conn_id=app_config["stock_api_connection_id"],
    endpoint=app_config["endpoint"],
    auth_endpoint=app_config["auth_endpoint"],
    date=yesterday,
    hdfs_file_path=bronze_file_path,
    username=username,
    password=password)

to_silver_task = SparkBronzeToSilverOperator(
    task_id="from_bronze_to_silver",
    dag=dag,
    date=yesterday,
    bronze_file_path=bronze_file_path,
    silver_file_path=silver_file_path,
    spark=spark,
    transformer="stock_api")

start_task >> to_bronze_task >> to_silver_task >> end_task
