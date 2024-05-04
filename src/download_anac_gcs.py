import requests
from google.cloud import storage
import os

serviceAccount = r"C:\Users\Thuany Vermelho\OneDrive\Área de Trabalho\chaves\project_dataflow.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

def download_and_upload(url, bucket_name, destination_blob_name):
    """Baixa um arquivo CSV da URL fornecida e o carrega no bucket do Google Cloud Storage (GCS).

    Args:
        url (str): A URL do arquivo CSV a ser baixado.
        bucket_name (str): O nome do bucket no GCS onde o arquivo será carregado.
        destination_blob_name (str): O caminho de destino do arquivo no GCS.

    Raises:
        requests.HTTPError: Se ocorrer um erro ao fazer o download do arquivo.
    """

    # Configurando o cliente de armazenamento
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Baixando o arquivo
    response = requests.get(url)
    response.raise_for_status()  # Garante que o download foi bem-sucedido

    # Salvando o arquivo no Google Cloud Storage
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(response.content, content_type='text/csv')

    print(f"Arquivo {destination_blob_name} carregado para {bucket_name}.")

# URL do arquivo CSV na ANAC
url = "https://www.gov.br/anac/pt-br/assuntos/dados-e-estatisticas/percentuais-de-atrasos-e-cancelamentos-2/2024/vra_2024_01.csv"

# Nome do bucket e do objeto no GCS
bucket_name = 'etl-dataflow-apache-beam'
destination_blob_name = 'input/vra_2024_01.csv'

# Chamando a função
download_and_upload(url, bucket_name, destination_blob_name)

print("Download com sucesso!")
