from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.hdfs_hook import HDFSHook
import logging


class DbToHdfsOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            db_conn_id="",
            hdfs_conn_id="",
            db_table_name="",
            db_custom_query="",
            *args, **kwargs):
        """
        :param db_conn_id: database connection id
        :param hdfs: hdfs connection id
        :param db_table_name: exported table
        :param db_custom_query: sql query if table is not provided
        """

        super().__init__(*args, **kwargs)
        self.db_conn = PostgresHook(db_conn_id).get_conn()
        self.hdfs_conn = HDFSHook(hdfs_conn_id).get_conn()
        self.db_table_name = db_table_name
        self.db_custom_query = db_custom_query

    def execute(self, context):
        logging.info("EXEC CUSTOM OPERATOR")
        logging.info(f"TABLE => {self.db_table_name}")
