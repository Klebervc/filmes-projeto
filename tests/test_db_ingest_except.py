import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from db.db_ingest import processar_filmes
from unittest.mock import patch, Mock, MagicMock

csv_filename = 'filmes.csv'
db_filename = 'filmes.db'
tabela_filmes = 'filmes'

def test_deve_dar_erro_quando_nao_criar_diretorio(tmp_path, capsys):
    
    path_csv = tmp_path / csv_filename
    
    processar_filmes(str(path_csv), '<>caminho/invalido.db')
    
    captured = capsys.readouterr()
    
    assert 'Erro na criação do diretório da base de dados: ' in captured.out

def test_deve_dar_erro_quando_csv_invalido(tmp_path, capsys):
    
    path_db = tmp_path / db_filename
    
    processar_filmes('caminho/invalido/arquivo.csv', str(path_db))
    
    captured = capsys.readouterr()
    
    assert 'O arquivo CSV não existe' in captured.out

def test_deve_dar_erro_quando_nao_ler_csv(tmp_path, capsys):
    
    path_db = tmp_path / db_filename
    
    csv_mal_formado = 'coluna_1,coluna_2\n"filme_1,filme_2\nfilme_3'
    
    with open('data/teste_mal_formado.csv', "w") as f:
        f.write(csv_mal_formado)
    
    processar_filmes('data/teste_mal_formado.csv', str(path_db))
    
    captured = capsys.readouterr()
    
    assert 'Erro ao ler o arquivo CSV' in captured.out

def test_deve_dar_erro_quando_nao_processar_csv(tmp_path, capsys):
    
    path_csv = tmp_path / csv_filename
    path_db = tmp_path / db_filename
    
    with open(str(path_csv), "w") as f:
        f.write("")
    
    with patch('pandas.read_csv', side_effect=OSError('Erro de sistema ao acessar arquivo')):
        processar_filmes(str(path_csv), str(path_db))
    
    captured = capsys.readouterr()
    
    assert 'Erro ao acessar arquivo CSV: Erro de sistema ao acessar arquivo' in captured.out

def test_deve_dar_demais_erros_com_arquivo_csv(tmp_path, capsys):
    
    path_csv = tmp_path / csv_filename    
    path_db = tmp_path / db_filename
    
    csv = "col_1,col_2\nfilme_1,filme_2"
    
    with open(str(path_csv), "w") as f:
        f.write(csv)
    
    with patch('pandas.read_csv', side_effect=RuntimeError('Erro inesperado simulado')):
        processar_filmes(str(path_csv), str(path_db))
    
    captured = capsys.readouterr()
    
    assert 'Erro inesperado ao carregar CSV: Erro inesperado simulado' in captured.out

def test_deve_dar_erro_quando_nao_salvar_tabela_1(tmp_path):
    
    path_csv = tmp_path / csv_filename
    path_db = tmp_path / db_filename
    
    csv = 'Titulo,Ano\nInception,2010'
    
    with open(str(path_csv), 'w') as f:
        f.write(csv)
    
    with patch('pandas.DataFrame.to_sql', side_effect=ValueError('Valor inválido')):
        resultado = processar_filmes(str(path_csv), str(path_db))
    
    print('resultado:', resultado)
    
    assert len(resultado['erros']) == 1

def test_deve_dar_erro_quando_nao_salvar_tabela_2(tmp_path):
    
    path_csv = tmp_path / csv_filename
    path_db = tmp_path / db_filename
    
    csv = 'Titulo,Ano\nInception,2010'
    
    with open(str(path_csv), 'w') as f:
        f.write(csv)
    
    with patch('pandas.DataFrame.to_sql', side_effect=ValueError('Valor inválido')):
        resultado = processar_filmes(str(path_csv), str(path_db))
    
    assert 'Valor inválido' in resultado['erros'][0]['erro']

def test_deve_dar_erro_quando_nao_salvar_tabela_3(tmp_path):
    
    path_csv = tmp_path / csv_filename
    path_db = tmp_path / db_filename
    
    csv = 'Titulo,Ano\nInception,2010'
    
    with open(str(path_csv), 'w') as f:
        f.write(csv)
    
    with patch('pandas.DataFrame.to_sql', side_effect=ValueError('Valor inválido')):
        resultado = processar_filmes(str(path_csv), str(path_db))
    
    assert resultado['erros'][0]['linha']['Titulo'] == 'Inception'

def test_deve_da_erro_quando_nao_salvar_tabela_banco(tmp_path, capsys):
    
    path_csv = tmp_path / csv_filename
    path_db = tmp_path / db_filename
    
    csv = 'Titulo,Ano\nInception,2010'
    
    with open(str(path_csv), 'w') as f:
        f.write(csv)
    
    with patch('pandas.DataFrame.to_sql', side_effect=SQLAlchemyError('Falha no Banco de Dados')):
        resultado = processar_filmes(str(path_csv), str(path_db))
    
    assert str(resultado[0]) == 'Falha no Banco de Dados'

def test_deve_dar_erro_quando_consulta_sql_invalida(tmp_path, capsys):
    
    path_csv = tmp_path / csv_filename
    path_db = tmp_path / db_filename
    
    csv = 'coluna_1,coluna_2\n1,2'
    
    with open(str(path_csv), 'w') as f:
        f.write(csv)
    
    mock_connection = MagicMock()
    mock_connection.execute.side_effect = SQLAlchemyError('Consulta SQL inválida')
    
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    
    with patch('db.db_ingest.create_engine', return_value=mock_engine):
        processar_filmes(str(path_csv), str(path_db))
    
    captured = capsys.readouterr()
    
    assert 'Erro ao realizar consulta SQL: Consulta SQL inválida' in captured.out

