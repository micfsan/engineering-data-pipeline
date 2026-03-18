from pyspark.sql import SparkSession

class SparkManager:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder \
            .appName(self.config.app_name) \
            .getOrCreate()

    def get_session(self):
        return self.spark