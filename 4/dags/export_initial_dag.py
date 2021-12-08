import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from config import Config

import logging


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

def export_from_db():
    conn = PostgresHook(app_config["pg_connection_id"]).get_conn()
    for table in ["aisles", "clients", "departments", "orders", "products"]:
        dir_path = os.path.join(app_config["directory"], "init")
        os.makedirs(dir_path, exist_ok=True)

        file_path = os.path.join(dir_path, f"{table}.csv")
        with open(file_path, "w") as f:
            conn.cursor().copy_expert(f"COPY (select * from {table}) TO STDOUT WITH CSV HEADER", f)
            logging.info(f"Saved file {file_path}")

dag = DAG(
    dag_id="export_initial_data",
    schedule_interval=None, # run dag only once manually
    start_date=datetime(2021, 12, 6, 0, 0),
    default_args=default_args
)

export_db=PythonOperator(
    task_id="export_from_db",
    dag=dag,
    python_callable=export_from_db
)

export_db
