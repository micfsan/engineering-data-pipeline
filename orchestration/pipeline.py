import logging

class Pipeline:
    """Classe de orquestração do pipeline[cite: 69]."""
    
    def __init__(self, data_io, business_logic):
        # Injeção das dependências necessárias [cite: 47, 53]
        self.data_io = data_io
        self.business_logic = business_logic
        self.logger = logging.getLogger(__name__)

    def run(self):
        """Executa a ordem das chamadas: Leitura -> Processamento -> Escrita[cite: 75]."""
        try:
            self.logger.info("Etapa: Leitura de dados iniciada.")
            df_pedidos = self.data_io.ler_pedidos()
            df_pagamentos = self.data_io.ler_pagamentos()

            self.logger.info("Etapa: Aplicando lógica de negócio (Filtros 2025/Fraude).")
            df_final = self.business_logic.processar_relatorio(df_pedidos, df_pagamentos)

            self.logger.info("Etapa: Gravando resultado em Parquet.")
            self.data_io.gravar_resultado(df_final)
            
            self.logger.info("Pipeline executado com sucesso!")
        except Exception as e:
            self.logger.error(f"Falha na execução do pipeline: {str(e)}")
            raise e