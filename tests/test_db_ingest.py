import pandas as pd
import pytest
from sqlalchemy import text
from db.db_ingest import processar_filmes

csv_filename = 'filmes.csv'
db_filename = 'filmes.db'
tabela_filmes = 'filmes'

@pytest.fixture
def definir_df_path_carregar_csv(tmp_path):
    
    df = pd.DataFrame({
        'Titulo': ['filme_1', 'filme_2'],
        'Ano': [2023, 2024],
        'Diretor': ['diretor_1', 'diretor_2']
    })
    
    path_csv = tmp_path / csv_filename
    path_db = tmp_path / db_filename
    
    df.to_csv(path_csv, index=False)
    
    return df, path_csv, path_db

@pytest.fixture
def testar_prints(capsys, definir_df_path_carregar_csv):
    
    _, path_csv, path_db = definir_df_path_carregar_csv
    
    processar_filmes(str(path_csv), str(path_db))
    
    captured = capsys.readouterr()
    
    return captured

def test_deve_garantir_diretorio_db_não_existe_quando_não_existir(tmp_path):
    
    path_db = tmp_path / 'subdir' / db_filename
    
    assert not path_db.parent.exists()

def test_deve_criar_diretorio_db_quando_processar_filmes(tmp_path, definir_df_path_carregar_csv):
    
    _, path_csv, _ = definir_df_path_carregar_csv
    
    path_db = tmp_path / 'subdir' / db_filename
    
    processar_filmes(str(path_csv), str(path_db))
    
    assert path_db.parent.exists()

def test_deve_criar_tabela_filmes_quando_criar_conexao_engine(definir_df_path_carregar_csv):
    
    _, path_csv, path_db = definir_df_path_carregar_csv
    
    engine = processar_filmes(str(path_csv), str(path_db)).get('engine')
    
    with engine.connect() as cnn:
        resultado = cnn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='filmes';"))
        tabela = [row[0] for row in resultado]
        
    assert tabela_filmes in tabela

def test_deve_comparar_dataframe_e_database_quando_criar_conexao_engine(definir_df_path_carregar_csv):
    
    df, path_csv, path_db = definir_df_path_carregar_csv
    
    engine = processar_filmes(str(path_csv), str(path_db)).get('engine')
    
    with engine.connect() as cnn:
        df_db = pd.read_sql(f"SELECT * FROM {tabela_filmes}", cnn)
        
    pd.testing.assert_frame_equal(df, df_db)

def test_deve_dar_erro_quando_nao_capturar_print_informações_tabela(testar_prints):
    
    captured = testar_prints
    
    assert "Informações da tabela 'filmes':" in captured.out

def test_deve_dar_erro_quando_nao_capturar_print_titulo(testar_prints):
    
    captured = testar_prints
    
    assert "Nome: Titulo" in captured.out

def test_deve_dar_erro_quando_nao_capturar_print_ano(testar_prints):
    
    captured = testar_prints
    
    assert "Nome: Ano" in captured.out

def test_deve_dar_erro_quando_nao_capturar_print_diretor(testar_prints):
    
    captured = testar_prints
    
    assert "Nome: Diretor" in captured.out