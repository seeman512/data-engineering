from abc import ABC, abstractmethod


class BaseTransformer(ABC):
    def __init__(self, spark, file_path):
        self.spark = spark
        self.file_path = file_path
        self.df = None

    @abstractmethod
    def transform(self):
        pass

