from io.schemas import schema_pedidos, schema_pagamentos

class DataIO:
    """Classe de leitura e escrita de dados (I/O)[cite: 61]."""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def ler_pedidos(self):
        """Lê o dataset de pedidos (CSV) com schema explícito[cite: 41, 99]."""
        return self.spark.read.csv(
            self.config.PATH_PEDIDOS, 
            header=True, 
            schema=schema_pedidos # Critério 1: Sem inferência [cite: 41]
        )

    def ler_pagamentos(self):
        """Lê o dataset de pagamentos (JSON) com schema explícito[cite: 41, 94]."""
        return self.spark.read.json(
            self.config.PATH_PAGAMENTOS, 
            schema=schema_pagamentos # Critério 1: Sem inferência [cite: 41]
        )

    def gravar_resultado(self, df):
        """Grava o relatório final em formato Parquet[cite: 18]."""
        df.write.mode("overwrite").parquet(self.config.PATH_OUTPUT)