import pandas as pd
import pytest
import os
from sqlalchemy import text
from db.db_ingest import processar_filmes

def test_deve_criar_diretorio_db(tmp_path):
    
    df = pd.DataFrame({
        'Titulo': ['filme_1', 'filme_2'],
        'Ano': [2023, 2024],
        'Diretor': ['diretor_1', 'diretor_2']
    })
    
    path_csv = tmp_path / 'filmes.csv'
    path_db = tmp_path / 'subdir' / 'filmes.db'
    
    df.to_csv(path_csv, index=False)
    
    assert not path_db.parent.exists()
    
    processar_filmes(str(path_csv), str(path_db))
    
    assert path_db.parent.exists()

def test_deve_testar_engine_tabela(tmp_path):
    
    df = pd.DataFrame({
        'Titulo': ['filme_1', 'filme_2'],
        'Ano': [2023, 2024],
        'Diretor': ['diretor_1', 'diretor_2']
    })
    
    path_csv = tmp_path / 'filmes.csv'
    path_db = tmp_path / 'filmes.db'
    
    df.to_csv(path_csv, index=False)
    
    engine = processar_filmes(str(path_csv), str(path_db))
    
    with engine.connect() as cnn:
        resultado = cnn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='filmes';"))
        tabela = [row[0] for row in resultado]
        
    assert "filmes" in tabela

def test_deve_testar_dataframe_database(tmp_path):
    
    df = pd.DataFrame({
        'Titulo': ['filme_1', 'filme_2'],
        'Ano': [2023, 2024],
        'Diretor': ['diretor_1', 'diretor_2']
    })
    
    path_csv = tmp_path / 'filmes.csv'
    path_db = tmp_path / 'filmes.db'
    
    df.to_csv(path_csv, index=False)
    
    engine = processar_filmes(str(path_csv), str(path_db))
    
    with engine.connect() as cnn:
        df_db = pd.read_sql("SELECT * FROM filmes", cnn)
        
    pd.testing.assert_frame_equal(df, df_db)

def test_deve_testar_colunas_print(tmp_path, capsys):
    
    df = pd.DataFrame({
        'Titulo': ['filme_1', 'filme_2'],
        'Ano': [2023, 2024],
        'Diretor': ['diretor_1', 'diretor_2']
    })
    
    path_csv = tmp_path / 'filmes.csv'
    path_db = tmp_path / 'filmes.db'
    
    df.to_csv(path_csv, index=False)
    
    processar_filmes(str(path_csv), str(path_db))
    
    captured = capsys.readouterr()
    assert "Informações da tabela 'filmes':" in captured.out
    assert "Nome: Titulo" in captured.out
    assert "Nome: Ano" in captured.out
    assert "Nome: Diretor" in captured.out