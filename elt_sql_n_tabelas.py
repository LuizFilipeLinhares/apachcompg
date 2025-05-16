import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from sqlalchemy import create_engine

import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

def elt_sql_postgresql_to_adls():
    # Carregar variáveis de ambiente do arquivo .env
    load_dotenv()

    # Configurações do Azure Data Lake Storage
    account_name = os.getenv("ADLS_ACCOUNT_NAME")
    file_system_name = os.getenv("ADLS_FILE_SYSTEM_NAME")
    directory_name = os.getenv("ADLS_DIRECTORY_NAME")
    sas_token = os.getenv("ADLS_SAS_TOKEN")

    # Configurações do PostgreSQL
    host = os.getenv("PG_HOST")
    database = os.getenv("PG_DATABASE")
    schema = os.getenv("PG_SCHEMA")
    username = os.getenv("PG_USERNAME")
    password = os.getenv("PG_PASSWORD")
    port = os.getenv("PG_PORT", "5432")  # Porta padrão do PostgreSQL é 5432

    # Supondo que você tenha a senha armazenada em uma variável chamada 'password'
    password = quote_plus(password)

    # Conectar ao PostgreSQL
    conn_str = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

    # Criar a engine do SQLAlchemy
    engine = create_engine(conn_str)

    # Consulta SQL para obter todas as tabelas do esquema
    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"

    # Criar cliente do Azure Data Lake Storage
    file_system_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", 
                                               credential=sas_token,
                                               api_version="2020-02-10")

    # Tentar criar o diretório, se não existir
    try:
        directory_client = file_system_client.get_file_system_client(file_system_name).get_directory_client(directory_name)
        directory_client.create_directory()
    except ResourceExistsError:
        print(f"O diretório '{directory_name}' já existe.")

    # Executar a consulta para obter todas as tabelas do esquema
    tables_df = pd.read_sql(query, engine)

    # Para cada tabela encontrada, ler os dados e carregar para o Azure Data Lake Storage
    for index, row in tables_df.iterrows():
        table_name = row["table_name"]
        query = f"SELECT * FROM {schema}.{table_name}"
        df = pd.read_sql(query, engine)
        
        # Carregar os dados para o Azure Data Lake Storage
        file_client = directory_client.get_file_client(f"{table_name}.csv")
        data = df.to_csv(index=False).encode()
        file_client.upload_data(data, overwrite=True)
        print(f"Dados da tabela '{table_name}' carregados com sucesso.")
