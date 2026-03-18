import pytest
from pyspark.sql import SparkSession
from business.logic import BusinessLogic
from datetime import date

@pytest.fixture(scope="session")
def spark():
    # O método correto é getOrCreate()
    return SparkSession.builder.master("local[1]").appName("TestingLogic").getOrCreate()

def test_processar_relatorio_filtros(spark):
    logic = BusinessLogic()
    
    # Criando dados fictícios para validar os filtros de 2025 e fraude
    data_pedidos = [
        ("1", "SP", 100.0, date(2025, 1, 1)), 
        ("2", "RJ", 200.0, date(2024, 12, 31)) 
    ]
    df_pedidos = spark.createDataFrame(data_pedidos, ["id_pedido", "uf", "valor_total", "data_pedido"])
    
    data_pagamentos = [
        ("1", "Cartão", False, False), 
        ("2", "Boleto", False, False)  
    ]
    df_pagamentos = spark.createDataFrame(data_pagamentos, ["id_pedido", "forma_pagamento", "status", "fraude"])
    
    # Executa a lógica de negócio (Critério 7)
    resultado = logic.processar_relatorio(df_pedidos, df_pagamentos)
    
    # Validação: deve retornar apenas 1 linha (o pedido de 2025)
    assert resultado.count() == 1
    assert resultado.first()["id_pedido"] == "1"
