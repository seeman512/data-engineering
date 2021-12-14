from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook

from hdfs import InsecureClient
import logging


class DbToHdfsOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            db_conn_id="",
            hdfs_conn_id="",
            db_table_name="",
            db_custom_query="",
            hdfs_file_path="",
            *args, **kwargs):
        """
        :param db_conn_id: database connection id
        :param hdfs: hdfs connection id
        :param db_table_name: exported table
        :param db_custom_query: sql query if table is not provided
        :param hdfs_file_path: hdfs file path
        """

        super().__init__(*args, **kwargs)
        self.db_conn = PostgresHook(db_conn_id).get_conn()
        self.db_table_name = db_table_name
        self.db_custom_query = db_custom_query
        self.hdfs_file_path = hdfs_file_path

        hdfs_conn = BaseHook.get_connection(hdfs_conn_id)
        self.hdfs_client = InsecureClient(f"http://{hdfs_conn.host}:{hdfs_conn.port}")

    def execute(self, context):
        if self.db_table_name:
            query = f"select * from {self.db_table_name}"
        else:
            query = self.db_custom_query

        with self.hdfs_client.write(self.hdfs_file_path, overwrite=True) as f:
            logging.info(f"Writing to {self.hdfs_file_path}")
            self.db_conn.cursor().copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", f)
