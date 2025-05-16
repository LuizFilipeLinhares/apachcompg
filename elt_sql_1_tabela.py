import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError

import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do PostgreSQL
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
database = os.getenv("PG_DATABASE")
schema = os.getenv("PG_SCHEMA")
table_name = os.getenv("PG_TABLE_NAME")
username = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")

# Configurações do Azure Data Lake Storage
account_name = os.getenv("ADLS_ACCOUNT_NAME")
file_system_name = os.getenv("ADLS_FILE_SYSTEM_NAME")
directory_name = database
sas_token = os.getenv("ADLS_SAS_TOKEN")

from urllib.parse import quote_plus

# Supondo que você tenha a senha armazenada em uma variável chamada 'password'
password = quote_plus(password)

# Consulta SQL
query = f"SELECT * FROM {schema}.{table_name}"

# Conectar ao PostgreSQL e ler os dados
conn_str = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
engine = create_engine(conn_str)
df = pd.read_sql(query, engine)

# Escrever os dados no Azure Data Lake Storage
file_system_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", 
                                           credential=sas_token,
                                           api_version="2020-02-10")  # Definir a versão da API explicitamente

# Tentar criar o diretório, se não existir
try:
    directory_client = file_system_client.get_file_system_client(file_system_name).get_directory_client(directory_name)
    directory_client.create_directory()
except ResourceExistsError:
    print(f"O diretório '{directory_name}' já existe.")

# Carregar o arquivo para o Azure Data Lake Storage
file_client = directory_client.get_file_client(f"{table_name}.csv")

# Criar o arquivo
file_client.create_file()

# Converter DataFrame para CSV e obter os dados como bytes
data = df.to_csv(index=False).encode()

# Carregar os dados
file_client.upload_data(data, overwrite=True)
