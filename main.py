cat <<EOF > main.py
from config.config import Config
from spark_session.spark_manager import SparkManager
from io.data_io import DataIO
from business.logic import BusinessLogic
from orchestration.pipeline import Pipeline

def main():
    # 1. Instanciar Configurações (Critério 4)
    config = Config()
    
    # 2. Instanciar Gerenciador Spark (Critério 5)
    spark_manager = SparkManager(config)
    spark = spark_manager.get_session()
    
    # 3. Instanciar I/O (Critério 6)
    data_io = DataIO(spark, config)
    
    # 4. Instanciar Lógica de Negócios (Critério 7)
    business_logic = BusinessLogic()
    
    # 5. Instanciar Orquestrador (Critério 8) - Injeção de Dependências
    pipeline = Pipeline(data_io, business_logic)
    
    # Executar o Pipeline
    pipeline.run()

if __name__ == "__main__":
    main()
EOF