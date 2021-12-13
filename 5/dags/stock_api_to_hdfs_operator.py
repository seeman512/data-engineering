from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.models.connection import Connection

from hdfs import InsecureClient
import json
import logging


class StockApiToHdfsOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            http_conn_id="",
            hdfs_conn_id="",
            hdfs_file_path="",
            auth_endpoint="",
            endpoint="",
            username="",
            password="",
            date="",
            *args, **kwargs):
        """
        :param http_conn_id: api connection id
        :param hdfs: hdfs connection id
        :param hdfs_file_path: hdfs file path
        :param auth_endpoint: token endpoint
        :param endpoint: data endpoint
        :param username: auth endpoint username
        :param password: auth endpoint password
        :param date: action date
        """

        super().__init__(*args, **kwargs)
        self.hdfs_file_path = hdfs_file_path

        hdfs_conn = BaseHook.get_connection(hdfs_conn_id)
        self.hdfs_client = InsecureClient(f"http://{hdfs_conn.host}:{hdfs_conn.port}")

        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.auth_endpoint = auth_endpoint
        self.username = username
        self.password = password
        self.token = None

        self.date = date
    
    def execute(self, context):
        if not self.token:
            self._set_token()

        http_client = HttpHook("GET", http_conn_id=self.http_conn_id)
        data = {"date": self.date}
        headers = {"content-type": "application/json",
                   "Authorization": f"JWT {self.token}"}

        r = http_client.run(self.endpoint, headers=headers,
                            extra_options={"check_response": False},
                            json=data)

        if r.status_code == 401:
            logging.info("Token expired")
            self._set_token()

            headers["Authorization"] = f"JWT {self.token}"
            r = http_client.run(self.endpoint, headers=headers,
                                json=data)

        http_client.check_response(r)

        with self.hdfs_client.write(self.hdfs_file_path, overwrite=True, encoding="utf-8") as f:
            logging.info(f"Writing to {self.hdfs_file_path}")
            json.dump(r.json(), f)

    def _set_token(self):
        headers={"content-type": "application/json"}
        http_client = HttpHook("POST", http_conn_id=self.http_conn_id)
        data = {"username": self.username, "password": self.password}

        r = http_client.run(self.auth_endpoint, headers=headers,
                            data=json.dumps(data))

        http_client.check_response(r)

        self.token = r.json()["access_token"]
        logging.info("Generated new token")
