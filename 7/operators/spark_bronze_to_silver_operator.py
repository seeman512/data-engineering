import importlib
import inspect
import logging

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException


class SparkBronzeToSilverOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            bronze_file_path="",
            silver_file_path="",
            transformer="",
            spark=None,
            *args, **kwargs):
        """
        :param bronze_file_path: bronze file path
        :param silver_file_path: silver file path
        :param transformer: spark dataframe transformer
        :param spark: spark session object
        """

        super().__init__(*args, **kwargs)
        self.silver_file_path = silver_file_path

        # load transformer
        module_name = f"{transformer}_transformer"
        module = importlib.import_module(f"spark_transformers.{module_name}")
        className = "".join(map(lambda s: s.capitalize(), module_name.split("_")))
        transformer_class = getattr(module, className)

        if not inspect.isclass(transformer_class):
            raise AirflowException(f"Could not load f{transformer} transformer")
        
        logging.info(f"Loaded {className}")
        self.transformer = transformer_class(spark, bronze_file_path)
    
    def execute(self, context):
        self.transformer\
            .transform()\
            .write\
            .parquet(self.silver_file_path, mode='overwrite')
        logging.info(f"Saved {self.silver_file_path}")
