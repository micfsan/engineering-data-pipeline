from io_package.schemas import pedidos_schema, pagamentos_schema

class DataIO:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def ler_pedidos(self):
        # O Spark varre a pasta e lê todos os CSVs de uma vez
        return self.spark.read.json("data/pedidos/", schema=pedidos_schema)

    def ler_pagamentos(self):
        # O Spark varre a pasta e lê todos os JSONs de uma vez
        return self.spark.read.json("data/pagamentos/", schema=pagamentos_schema)

    def gravar_resultado(self, df):
        df.write.mode("overwrite").parquet(self.config.output_path)
