import os
import sys
from datetime import datetime

from airflow import DAG
from config import Config

import logging

from db_to_hdfs_operator import DbToHdfsOperator


config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "config.yaml")
app_name = "app"
cfg = Config(config_path)
app_config = cfg.app_config(app_name)

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'retries': 2,
}

dag = DAG(
    dag_id="export_initial_data",
    schedule_interval=None, # run dag only once manually
    start_date=datetime(2021, 12, 6, 0, 0),
    default_args=default_args
)

tables = ["aisles", "clients", "departments", "location_areas", "orders",
          "products", "store_types", "stores"]

task_kwargs = {
    "dag": dag,
    "db_conn_id": app_config["db_connection_id"],
    "hdfs_conn_id": app_config["hdfs_connection_id"],
}

[ DbToHdfsOperator(task_id=f"export_from_db_{table}",
                   db_table_name=table,
                   hdfs_file_path=os.path.join(app_config['directory'], 'init', f"{table}.csv"),
                   **task_kwargs)
      for table in tables ]
