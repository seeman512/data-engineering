import os
import sys
from datetime import datetime

from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from api_client import ApiClient
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
    'retries': 2
}

def export_from_api():
    from api_client import ApiClient

    app_config['username'] = Variable.get("app_username", app_config["username"])
    app_config['password'] = Variable.get("app_password", app_config["password"])

    if not app_config['username'] or not app_config['password']:
        raise AirflowException("Require api username or password in airflow variables or config.yaml")

    client = ApiClient(app_config)
    date = "2021-04-01"
    data = client.get_data(date)
    logging.info("DAATA")
    logging.info(data)

def export_from_db():
    conn = PostgresHook(app_config["pg_connection_id"]).get_conn()
    with open("/home/user/ISLES.csv", "w") as f:
        conn.cursor().copy_expert("COPY (select * from aisles) TO STDOUT WITH CSV HEADER", f)

dag = DAG(
    dag_id="export_data",
    schedule_interval="@daily",
    start_date=datetime(2021, 12, 6, 0, 0),
    default_args=default_args
)

# export_api=PythonOperator(
#     task_id="export_from_api",
#     dag=dag,
#     python_callable=export_from_api
# )

export_db=PythonOperator(
    task_id="export_from_db",
    dag=dag,
    python_callable=export_from_db
)

export_db
