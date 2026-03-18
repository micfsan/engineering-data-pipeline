from pyspark.sql import SparkSession

class SparkManager:
    """Classe de gerenciamento de sessão spark."""
    
    def __init__(self, config):
        # Recebe a configuração via injeção de dependência [cite: 46]
        self.app_name = "MBA_FIAP_Engineering_Data_Pipeline"
        self._spark = None

    def get_session(self):
        """Cria ou retorna a sessão spark ativa."""
        if not self._spark:
            self._spark = (SparkSession.builder 
                          .appName(self.app_name) 
                          .get_session())
        return self._spark