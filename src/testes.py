from google.cloud import bigquery
from datetime import datetime
import os


# Configuração da credencial de serviço do Google Cloud
serviceAccount = r"C:\Users\Thuany Vermelho\OneDrive\Área de Trabalho\chaves\project_dataflow.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount


def preprocess(line):
    cols = line.split(';')
    
    # Convertendo datas e horas para o formato desejado
    partida_prevista = convert_to_datetime(cols[5])
    partida_real = convert_to_datetime(cols[6]) if cols[6] else None  # Verifica se o campo está vazio
    chegada_prevista = convert_to_datetime(cols[8])
    chegada_real = convert_to_datetime(cols[9]) if cols[9] else None  # Verifica se o campo está vazio

       
    # Construindo o dicionário com os dados preprocessados
    preprocessed_data = {
        'Sigla_ICAO_Empresa_Aerea': cols[0],
        'Número_Voo': cols[1],
        'Código_DI': cols[2],
        'Código_Tipo_Linha': cols[3],
        'Sigla_ICAO_Aeroporto_Origem': cols[4],
        'Partida_Prevista': partida_prevista,
        'Partida_Real': partida_real,
        'Sigla_ICAO_Aeroporto_Destino': cols[7],
        'Chegada_Prevista': chegada_prevista,
        'Chegada_Real': chegada_real,
        'Situação_Voo': cols[10]
    }

    return preprocessed_data

def convert_to_datetime(date_str):
    if date_str:
        try:
            return datetime.strptime(date_str, '%d/%m/%Y %H:%M')
        except ValueError:
            # Se ocorrer um erro ao converter a data, retorne None
            return None
    else:
        return None

def insert_into_bigquery(preprocessed_data, table_id):
    client = bigquery.Client()
    table = client.get_table(table_id)

    rows_to_insert = [preprocessed_data]

    errors = client.insert_rows(table, rows_to_insert)

    if errors == []:
        print("Dados inseridos com sucesso no BigQuery.")
    else:
        print("Erro ao inserir dados no BigQuery:", errors)

# Exemplo de uso

# Exemplo de uso
line = "AAL;995;0;I;KMIA;01/03/2024 00:45;01/03/2024 00:55;SBGR;01/03/2024 09:15;01/03/2024 09:10;REALIZADO"
preprocessed_data = preprocess(line)
table_id = "project-gcp-421011.voos_Anac_2024.voos"
insert_into_bigquery(preprocessed_data, table_id)