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

from operators.spark_bronze_to_silver_operator import SparkBronzeToSilverOperator
from operators.db_to_hdfs_operator import DbToHdfsOperator
from config import Config


config_path = os.path.join(parent_dir, "config.yaml")
app_name = "app"
cfg = Config(config_path)
app_config = cfg.app_config(app_name)

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'retries': 2,
}

yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

spark_conn = BaseHook.get_connection(app_config["spark_connection_id"])
spark = SparkSession.builder\
    .master(spark_conn.host)\
    .appName("stock_api_dag")\
    .getOrCreate()

dag = DAG(
    dag_id="orders_db",
    schedule_interval="@daily",
    default_args=default_args,
    start_date=datetime(2021, 12, 6, 0, 0),
)

start_task = DummyOperator(
    task_id='start_orders_db',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_orders_db',
    dag=dag,
)

for table in app_config["tables"]:
    bronze_file_path = os.path.join(app_config["bronze_directory"],
                                    "orders_db",
                                    yesterday,
                                    f"{table}.csv")

    silver_file_path = os.path.join(app_config["silver_directory"],
                                    f"db_{table}")
    start_task\
        >> DbToHdfsOperator(
            task_id=f"from_{table}_to_bronze",
            dag=dag,
            db_conn_id=app_config["db_connection_id"],
            hdfs_conn_id=app_config["hdfs_connection_id"],
            db_table_name=table,
            hdfs_file_path=bronze_file_path)\
        >> SparkBronzeToSilverOperator(
            task_id=f"from_{table}_bronze_to_silver",
            dag=dag,
            date=yesterday,
            bronze_file_path=bronze_file_path,
            silver_file_path=silver_file_path,
            spark=spark,
            transformer=f"db_{table}")\
        >> end_task
