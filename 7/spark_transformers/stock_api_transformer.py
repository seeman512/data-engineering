from .base_transformer import BaseTransformer


class StockApiTransformer(BaseTransformer):
    
    def __init__(self, spark, file_path, *args, **kwargs):
        super().__init__(spark, file_path, *args, **kwargs)

    def transform(self):
        return self.spark.read.json(self.file_path)\
            .drop("date")\
            .dropDuplicates()
