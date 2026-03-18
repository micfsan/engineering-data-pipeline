cat <<EOF > config/config.py
class Config:
    def __init__(self):
        # Caminhos dos datasets conforme material de apoio [cite: 94, 99]
        self.PATH_PEDIDOS = "datasets-csv-pedidos/data/pedidos/"
        self.PATH_PAGAMENTOS = "dataset-json-pagamentos/data/pagamentos/"
        self.PATH_OUTPUT = "output/relatorio_vendas_2025.parquet" [cite: 18]
EOF