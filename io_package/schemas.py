from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

# Schema para os Pedidos (Critério 1 - Tipagem Explícita)
pedidos_schema = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("id_cliente", StringType(), True),
    StructField("data_pedido", TimestampType(), True),
    StructField("valor_total", DoubleType(), True),
    StructField("uf", StringType(), True)
])

# Schema para os Pagamentos
pagamentos_schema = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_pagamento", DoubleType(), True),
    StructField("status", BooleanType(), True),
    StructField("fraude", BooleanType(), True)
])