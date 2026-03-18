import logging
from config.config import Config
from spark_session.spark_manager import SparkManager
from io_package.data_io import DataIO
from business.logic import BusinessLogic
from orchestration.pipeline import Pipeline

def main():
    # Configuração de Log (Critério 9)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    try:
        # 1. Instanciar Configurações (Critério 4)
        config = Config()
        
        # 2. Instanciar Gerenciador Spark (Critério 5)
        spark_manager = SparkManager(config)
        spark = spark_manager.get_session()
        
        # 3. Instanciar I/O (Critério 6)
        data_io = DataIO(spark, config)
        
        # 4. Instanciar Lógica de Negócios (Critério 7)
        business_logic = BusinessLogic()
        
        # 5. Instanciar Orquestrador (Critério 3 - Injeção de Dependências)
        pipeline = Pipeline(data_io, business_logic)
        
        # Executar o Pipeline (Critério 14)
        logger.info("Iniciando execução do pipeline principal...")
        pipeline.run()
        logger.info("Pipeline finalizado com sucesso!")

    except Exception as e:
        logger.error(f"Erro fatal na execução: {str(e)}")

if __name__ == "__main__":
    main()