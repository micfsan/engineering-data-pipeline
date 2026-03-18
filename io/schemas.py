from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, DateType

# Schema para o Dataset de Pedidos (CSV) [cite: 98]
schema_pedidos = StructType([
    StructField("id_pedido", StringType(), False),
    StructField("uf", StringType(), True),
    StructField("valor_total", DoubleType(), True),
    StructField("data_pedido", DateType(), True)
])

# Schema para o Dataset de Pagamentos (JSON) [cite: 93]
schema_pagamentos = StructType([
    StructField("id_pedido", StringType(), False),
    StructField("forma_pagamento", StringType(), True),
    StructField("status", BooleanType(), True),
    StructField("fraude", BooleanType(), True)
])
EOF