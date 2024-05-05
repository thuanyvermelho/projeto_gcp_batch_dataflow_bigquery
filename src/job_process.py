import apache_beam as beam
import os
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions

# Configurações da pipeline
pipeline_options = {
    'project': 'project-gcp-421011',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://etl-dataflow-apache-beam/temp',
    'temp_location': 'gs://etl-dataflow-apache-beam/temp',
    'template_location': 'gs://etl-dataflow-apache-beam/template/job_voos_gcs_batch',
    'save_main_session': True
}

# Inicializa as opções da pipeline
options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=options)

# Configuração da credencial de serviço do Google Cloud
serviceAccount = r"C:\Users\Thuany Vermelho\OneDrive\Área de Trabalho\chaves\project_dataflow.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

def preprocess(line):
    """Realiza o pré-processamento de uma linha do arquivo CSV de voos da ANAC.

    Args:
        line (str): Uma linha do arquivo CSV de voos da ANAC.

    Returns:
        dict: Um dicionário contendo os dados pré-processados da linha.
    """
    cols = line.split(';')
    
    # Convertendo datas e horas para o formato desejado
    partida_prevista = convert_to_datetime(cols[5])
    partida_real = convert_to_datetime(cols[6]) if cols[6] else None  # Verifica se o campo está vazio
    chegada_prevista = convert_to_datetime(cols[8])
    chegada_real = convert_to_datetime(cols[9]) if cols[9] else None  # Verifica se o campo está vazio

       
    # Construindo o dicionário com os dados pré-processados
    preprocessed_data = {
        'Sigla_ICAO_Empresa_Aerea': cols[0],
        'Numero_Voo': cols[1],
        'Codigo_DI': cols[2],
        'Codigo_Tipo_Linha': cols[3],
        'Sigla_ICAO_Aeroporto_Origem': cols[4],
        'Partida_Prevista': partida_prevista,
        'Partida_Real': partida_real,
        'Sigla_ICAO_Aeroporto_Destino': cols[7],
        'Chegada_Prevista': chegada_prevista,
        'Chegada_Real': chegada_real,
        'Situacao_Voo': cols[10]
    }

    return preprocessed_data


def convert_to_datetime(date_str):
    """Converte uma string de data e hora para um objeto datetime.

    Args:
        date_str (str): A string contendo a data e hora a serem convertidas.

    Returns:
        datetime: O objeto datetime correspondente à string de entrada.
            Retorna None se a string estiver vazia ou se a conversão falhar.
    """
    if date_str:
        try:
            return datetime.strptime(date_str, '%d/%m/%Y %H:%M')
        except ValueError:
            # Se ocorrer um erro ao converter a data, retorne None
            return None
    else:
        return None

# Pipeline de processamento de dados
processed_data = (
    p1
    | 'Leitura do CSV' >> beam.io.ReadFromText('gs://etl-dataflow-apache-beam/input/vra_2024_01.csv', skip_header_lines=1)
    | 'Pré-processamento' >> beam.Map(preprocess)
    | 'Escrita no BigQuery' >> beam.io.WriteToBigQuery(
        'project-gcp-421011.voos_Anac_2024.voos',
        schema='Sigla_ICAO_Empresa_Aerea:STRING, Numero_Voo:STRING, Codigo_DI:STRING, Codigo_Tipo_Linha:STRING, Sigla_ICAO_Aeroporto_Origem:STRING, Partida_Prevista:TIMESTAMP, Partida_Real:TIMESTAMP, Sigla_ICAO_Aeroporto_Destino:STRING, Chegada_Prevista:TIMESTAMP, Chegada_Real:TIMESTAMP, Situacao_Voo:STRING',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location=('gs://etl-dataflow-apache-beam/temp'))
)

# Executa a pipeline e aguarda sua conclusão
result = p1.run()
result.wait_until_finish()

print("Pipeline executado com sucesso!")
