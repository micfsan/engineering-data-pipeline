import logging
from pyspark.sql import functions as F

class BusinessLogic:
    def __init__(self):
        # Configuração do logging exigida pelo Critério 9
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def processar_relatorio(self, df_pedidos, df_pagamentos):
        """
        Executa a lógica: filtros de fraude (false), status (false) e ano 2025.
        """
        try:
            self.logger.info("Iniciando o processamento da lógica de negócio...")

            # 1. Filtro de pagamentos recusados e legítimos (Critério de Escopo)
            df_pagos_filtrados = df_pagamentos.filter(
                (F.col("status") == False) & (F.col("fraude") == False)
            )

            # 2. Join entre Pedidos e Pagamentos
            df_join = df_pedidos.join(df_pagos_filtrados, "id_pedido")

            # 3. Filtro para o ano de 2025 (Critério de Escopo)
            df_final = df_join.filter(F.year(F.col("data_pedido")) == 2025)

            # 4. Ordenação: UF, Forma Pagamento e Data (Critério de Escopo)
            df_final = df_final.sort("uf", "forma_pagamento", "data_pedido")

            self.logger.info("Processamento concluído com sucesso.")
            return df_final

        except Exception as e:
            # Registro do erro conforme Critério 10
            self.logger.error(f"Erro capturado na lógica de negócio: {str(e)}")
            raise e
