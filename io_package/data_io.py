from io_package.schemas import pedidos_schema, pagamentos_schema

class DataIO:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def ler_pedidos(self):
        return self.spark.read.csv(self.config.pedidos_path, schema=pedidos_schema, header=True)

    def ler_pagamentos(self):
        return self.spark.read.csv(self.config.pagamentos_path, schema=pagamentos_schema, header=True)

    def gravar_resultado(self, df):
        df.write.mode("overwrite").parquet(self.config.output_path)