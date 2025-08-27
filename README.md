# Projeto Apache Airflow - Qualidade e Análise de Dados

Este projeto demonstra como utilizar o Apache Airflow com Python para criar pipelines de dados que realizam verificações de qualidade e exploram dados de um endpoint público da cidade de Nova York.

O projeto é composto por duas DAGs (Directed Acyclic Graphs) que exemplificam fluxos de trabalho distintos:

- **Uma DAG que realiza uma validação básica de quantidade de registros.**
- **Uma DAG que executa uma análise mais aprofundada dos dados para verificar sua consistência e integridade.**

---

## 🧩 Explicação do Código

O arquivo da DAG (`dag_qualidade_e_analise.py`) contém duas DAGs independentes:

### 1. `primeira_dag_valida_registros`

Esta DAG atua como um "portão de qualidade". Seu objetivo é validar se o volume de dados extraído da fonte atende a uma expectativa mínima.

- **`captura_conta_dados`**: Utiliza `requests` e `pandas` para se conectar à URL, carregar o JSON em um DataFrame e retornar a quantidade de registros.
- **`e_valida` (BranchOperator)**: Puxa o valor da contagem (via XCom) e, com base em uma condição (> 1000), decide qual caminho a DAG deve seguir.
- **`valido` / `nvalido`**: Tarefas simples que representam os fluxos de sucesso ou falha, respectivamente. Em um cenário real, o caminho `nvalido` poderia acionar um alerta por e-mail ou uma notificação.

---

### 2. `dag_exploracao_dados`

Esta DAG executa uma análise exploratória dos dados para garantir sua qualidade. As tarefas são executadas em paralelo para otimizar o tempo de processamento.

- **`get_data_as_dataframe`**: A tarefa inicial que extrai os dados brutos da URL e os carrega em um DataFrame.
- **`check_data_quality`**: Verifica a integridade dos dados, imprimindo informações como a contagem de valores nulos e os tipos de dados de cada coluna.
- **`check_for_duplicates`**: Analisa o DataFrame para identificar e reportar a presença de linhas duplicadas.
- **`analyze_categorical_data`**: Fornece insights sobre a distribuição de valores em colunas categóricas, como `meal_type` e `city`.

---

## 🛠️ Como Executar o Código

Siga estes passos para configurar e executar o projeto em seu ambiente local.

### 1. Pré-requisitos

Certifique-se de que você tem o **Python 3.8+** instalado.

---

### 2. Configuração do Ambiente Python

Crie um ambiente virtual e ative-o para isolar as dependências do projeto:

```bash
python -m venv venv
source venv/bin/activate  # No Windows, use `venv\Scripts\activate`
```

### 3. Instalação do Apache Airflow e Dependências

Instale o Airflow e as bibliotecas necessárias para o código (pandas, requests).

```bash
pip install "apache-airflow[celery,postgres,google,s3]==2.7.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.8.txt"
pip install pandas requests
```

### 4. Inicialização do Airflow

Inicialize o banco de dados e crie um usuário para acessar a interface web.

```bash
airflow db migrate
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

### 5. Configuração da DAG

Copie o código completo deste projeto e cole-o em um novo arquivo dentro da pasta de DAGs do seu Airflow (geralmente ~/airflow/dags/).

```bash
# Navegue até a pasta de DAGs do seu ambiente Airflow
cd ~/airflow/dags

# Crie o arquivo e cole o código
nano dag_qualidade_e_analise.py
```

### 6. Execução

Inicie o scheduler e o servidor web do Airflow em terminais separados.

```bash
# Terminal 1: Iniciar o scheduler
airflow scheduler

# Terminal 2: Iniciar o servidor web
airflow webserver
```

Após iniciar os serviços, acesse a interface web em http://localhost:8080, encontre as duas DAGs e ative-as.