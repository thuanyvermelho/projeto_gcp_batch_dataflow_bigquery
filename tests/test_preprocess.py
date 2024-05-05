import pytest
from datetime import datetime
from src.job_process import preprocess  

def test_preprocess_valid_number_as_string():
    """Testa o pré-processamento de uma linha válida do arquivo CSV de voos da ANAC.

    Verifica se os dados são corretamente pré-processados para uma linha com todos os campos preenchidos.

    Resultado Esperado:
        O resultado do pré-processamento deve corresponder ao dicionário esperado com os valores corretos.

    Caso de Teste:
        Testa a conversão de datas em string para datetime.
    """
    line = "AAL;904;0;I;SBGL;01/03/2024 23:55;01/03/2024 23:48;KMIA;02/03/2024 07:45;02/03/2024 08:18;REALIZADO"
    expected = {
        'Sigla_ICAO_Empresa_Aerea': 'AAL',
        'Numero_Voo': '904',  
        'Codigo_DI': '0',
        'Codigo_Tipo_Linha': 'I',
        'Sigla_ICAO_Aeroporto_Origem': 'SBGL',
        'Partida_Prevista': datetime(2024, 3, 1, 23, 55),  
        'Partida_Real': datetime(2024, 3, 1, 23, 48),  
        'Sigla_ICAO_Aeroporto_Destino': 'KMIA',
        'Chegada_Prevista': datetime(2024, 3, 2, 7, 45),  
        'Chegada_Real': datetime(2024, 3, 2, 8, 18),  
        'Situacao_Voo': 'REALIZADO'
    }
    assert preprocess(line) == expected

def test_preprocess_missing_data():
    """Testa o pré-processamento de uma linha com dados ausentes do arquivo CSV de voos da ANAC.

    Verifica se os dados são corretamente pré-processados para uma linha com a partida prevista e chegada real faltantes.

    Resultado Esperado:
        O resultado do pré-processamento deve corresponder ao dicionário esperado com os valores corretos e os campos faltantes preenchidos com None.

    Caso de Teste:
        Testa o tratamento de dados ausentes com a situação de voo como "CANCELADO".
    """
    line = "AAL;904;0;I;SBGL;;01/03/2024 23:55;KMIA;02/03/2024 07:45;;CANCELADO"
    expected = {
        'Sigla_ICAO_Empresa_Aerea': 'AAL',
        'Numero_Voo': '904',
        'Codigo_DI': '0',
        'Codigo_Tipo_Linha': 'I',
        'Sigla_ICAO_Aeroporto_Origem': 'SBGL',
        'Partida_Prevista': None,  
        'Partida_Real': datetime(2024, 3, 1, 23, 55),  
        'Sigla_ICAO_Aeroporto_Destino': 'KMIA',
        'Chegada_Prevista': datetime(2024, 3, 2, 7, 45),
        'Chegada_Real': None,
        'Situacao_Voo': 'CANCELADO'
    }
    assert preprocess(line) == expected
