# Data Engineering Programming - MBA FIAP

Este projeto consiste em um pipeline de dados desenvolvido em **PySpark** para o processamento de grandes volumes de pedidos e pagamentos. O objetivo principal é filtrar transações legítimas com pagamentos recusados para análise da gestão.

## 1. Escopo de Negócio
O pipeline realiza o cruzamento (Join) entre as bases de pedidos e pagamentos para gerar um relatório que contempla:
* Apenas pedidos do ano de **2025**.
* Pagamentos com **status = false** (recusados).
* Avaliação de **fraude = false** (legítimos).
* Ordenação por **UF**, **Forma de Pagamento** e **Data**.

## 2. Estrutura do Projeto
O projeto segue os princípios de Orientação a Objetos e Injeção de Dependências, organizado da seguinte forma:
* `main.py`: Ponto de entrada (Aggregation Root).
* `business/`: Lógica de processamento e filtros.
* `io_package/`: Schemas explícitos e leitura/escrita de dados.
* `config/`: Centralização de caminhos e parâmetros.
* `spark_session/`: Gerenciamento da sessão Spark.
* `tests/`: Testes unitários com Pytest.

## 3. Pré-requisitos
* **Python**: 3.9 ou superior.
* **Java**: JDK 17 (Obrigatório para o Spark).
* **Spark**: 3.x.

## 4. Como Executar
   1. Instale as dependências:
      ```bash
      pip install -r requirements.txt

   2. Configure o JAVA_HOME (caso necessário no macOS):
      ```bash
      export JAVA_HOME=$(/usr/libexec/java_home -v 17)

   3. Execute o pipeline principal:
      ```bash
      python main.py
      ```

## 5. Como rodar os Testes
Para validar a lógica de negócio, execute:
   ```bash
   pytest tests/test_logic.py
      ```