from __future__ import annotations
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator  
import pandas as pd
import requests
import json
import numpy as np

# --- Funções para a primeira DAG  ---

def captura_conta_dados():  #Captura dados de uma URL e retorna a quantidade de registros.

    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd

def valida_com_tendencia(ti):

    # Simula a busca de contagens de 5 dias anteriores em um banco de dados
    dados_historicos = [1450, 1510, 1480, 1550, 1470]
    
    # A contagem atual é puxada via XCom
    contagem_atual = ti.xcom_pull(task_ids='captura_conta_dados')
    
    # Converte para um array numpy para facilitar os cálculos
    historico = np.array(dados_historicos)
    media_historica = historico.mean()
    
    # Define um limite de tolerância (ex: 20% abaixo da média)
    limite_inferior = media_historica * 0.80
    
    print(f"Média histórica: {media_historica:.2f}")
    print(f"Limite inferior aceitável: {limite_inferior:.2f}")
    print(f"Contagem atual: {contagem_atual}")
    
    if contagem_atual >= limite_inferior:
        print("Validação: OK. Contagem dentro do esperado.")
        return 'valido'
    else:
        print("Validação: FALHA. Queda significativa na contagem de registros.")
        return 'nvalido'

# --- Funções para a segunda DAG (Análise e Exploração) ---

def get_data_as_dataframe():#Captura dados de uma URL e retorna um DataFrame do Pandas.
    
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    return df

def check_data_quality(ti):#Verifica informações básicas de qualidade do DataFrame, como tipos de dados e valores nulos.
    
    df = ti.xcom_pull(task_ids='captura_dados_para_analise')
    print("\n--- Informações sobre o DataFrame (df.info()) ---")
    df.info()
    print("\n--- Quantidade de valores nulos por coluna ---")
    print(df.isnull().sum())
    return True

def check_for_duplicates(ti):#Verifica e reporta o número de linhas duplicadas.
    
    df = ti.xcom_pull(task_ids='captura_dados_para_analise')
    num_duplicates = df.duplicated().sum()
    print(f"\n--- Total de linhas duplicadas: {num_duplicates} ---")
    return True

def analyze_categorical_data(ti):#Analisa a frequência de valores em colunas categóricas.
    
    df = ti.xcom_pull(task_ids='captura_dados_para_analise')
    print("\n--- Frequência dos tipos de refeição (meal_type) ---")
    print(df['meal_type'].value_counts())
    print("\n--- Frequência das cidades ---")
    print(df['city'].value_counts())
    return True

# --- Funções para a terceira DAG (Tratamento e Transformação) ---

def handle_missing_values(ti):#Identifica e preenche valores nulos em colunas selecionadas.
    
    df = ti.xcom_pull(task_ids='captura_dados_para_tratamento')

    df['meal_count'] = pd.to_numeric(df['meal_count'], errors='coerce')
    df['meal_count'] = df['meal_count'].fillna(df['meal_count'].median())

    df['average_daily_attendance'] = pd.to_numeric(df['average_daily_attendance'], errors='coerce')
    df['average_daily_attendance'] = df['average_daily_attendance'].fillna(df['average_daily_attendance'].median())
    
    df['school_type'] = df['school_type'].fillna('Desconhecido')

    print("Valores nulos tratados com sucesso.")
    return df

def convert_data_types(ti):#Converte colunas para os tipos de dados apropriados.
    
    df = ti.xcom_pull(task_ids='tratar_valores_nulos')
    
    df['meal_count'] = df['meal_count'].astype(float)
    df['average_daily_attendance'] = df['average_daily_attendance'].astype(float)
    df['as_of_date'] = pd.to_datetime(df['as_of_date'], errors='coerce')
    
    print("Tipos de dados convertidos com sucesso.")
    print(df.info())
    return df

def standardize_categorical_data(ti):#Padroniza strings em colunas categóricas para consistência.
    
    df = ti.xcom_pull(task_ids='converter_tipos_dados')

    df['school_type'] = df['school_type'].str.lower().str.strip()
    df['city'] = df['city'].str.lower().str.strip()
    
    print("Dados categóricos padronizados com sucesso.")
    return df

def filter_out_invalid_data(ti):#Filtra e remove registros inválidos ou atípicos.
    
    df = ti.xcom_pull(task_ids='padronizar_dados_categoricos')

    df_filtered = df[df['meal_count'] > 0].copy()
    
    num_rows_before = len(df)
    num_rows_after = len(df_filtered)
    
    print(f"Linhas antes do filtro: {num_rows_before}")
    print(f"Linhas após o filtro: {num_rows_after}")
    print(f"Total de linhas removidas: {num_rows_before - num_rows_after}")
    
    return df_filtered

def aggregate_data(ti):#Agrega dados em um nível mais alto para análise.
    
    df = ti.xcom_pull(task_ids='filtrar_dados_invalidos')
    
    df['as_of_date'] = pd.to_datetime(df['as_of_date'])
    df['month'] = df['as_of_date'].dt.month
    
    aggregated_df = df.groupby(['city', 'school_type', 'month']).agg(
        total_meal_count=('meal_count', 'sum'),
        total_attendance=('average_daily_attendance', 'sum')
    ).reset_index()
    
    print("DataFrame agregado criado com sucesso. Primeiras 5 linhas:")
    print(aggregated_df.head())
    return aggregated_df

# --- Definição da Primeira DAG: Validação de Registros com Tendência ---

with DAG(
    dag_id='dag_valida_registros_tendencia',
    start_date=datetime(2025, 8, 20),
    schedule_interval='30 * * * *',
    catchup=False,
    tags=['exemplo', 'branch']
) as dag:
    from airflow.operators.python import BranchPythonOperator
    captura_dados = PythonOperator(
        task_id='captura_conta_dados',
        python_callable=captura_conta_dados
    )

    valida_tendencia_task = BranchPythonOperator(
        task_id='valida_com_tendencia',
        python_callable=valida_com_tendencia
    )

    valido = BashOperator(
        task_id='valido',
        bash_command="echo 'Quantidade de registros está de acordo com a tendência. Processamento pode continuar.'"
    )

    nvalido = BashOperator(
        task_id='nvalido',
        bash_command="echo 'Atenção: Queda no volume de dados detectada. A pipeline foi interrompida.'"
    )
    
    captura_dados >> valida_tendencia_task >> [valido, nvalido]

# --- Definição da Segunda DAG: Exploração de Dados ---

with DAG(
    dag_id='dag_exploracao_dados',
    start_date=datetime(2025, 8, 20),
    schedule_interval='@daily',
    catchup=False,
    tags=['exemplo', 'analise']
) as dag:
    
    captura_dados_para_analise = PythonOperator(
        task_id='captura_dados_para_analise',
        python_callable=get_data_as_dataframe,
    )
    
    verificacao_qualidade = PythonOperator(
        task_id='verificacao_qualidade',
        python_callable=check_data_quality,
    )
    
    verificacao_duplicados = PythonOperator(
        task_id='verificacao_duplicados',
        python_callable=check_for_duplicates,
    )
    
    analise_categorica = PythonOperator(
        task_id='analise_categorica',
        python_callable=analyze_categorical_data,
    )
    
    captura_dados_para_analise >> [verificacao_qualidade, verificacao_duplicados, analise_categorica]

# --- Definição da Terceira DAG: Tratamento e Transformação ---

with DAG(
    dag_id='dag_tratamento_dados_etl',
    start_date=datetime(2025, 8, 20),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'tratamento']
) as dag:
    
    captura_dados_para_tratamento = PythonOperator(
        task_id='captura_dados_para_tratamento',
        python_callable=get_data_as_dataframe,
    )
    
    tratar_valores_nulos = PythonOperator(
        task_id='tratar_valores_nulos',
        python_callable=handle_missing_values,
    )
    
    converter_tipos_dados = PythonOperator(
        task_id='converter_tipos_dados',
        python_callable=convert_data_types,
    )

    padronizar_dados_categoricos = PythonOperator(
        task_id='padronizar_dados_categoricos',
        python_callable=standardize_categorical_data,
    )

    filtrar_dados_invalidos = PythonOperator(
        task_id='filtrar_dados_invalidos',
        python_callable=filter_out_invalid_data,
    )

    agregar_dados = PythonOperator(
        task_id='agregar_dados',
        python_callable=aggregate_data,
    )
    
    # Encadeamento das tarefas
    captura_dados_para_tratamento >> tratar_valores_nulos >> converter_tipos_dados >> padronizar_dados_categoricos >> filtrar_dados_invalidos >> agregar_dados