import pandas as pd
from sqlalchemy import create_engine, text

path_csv = 'data/processed/filmes_limpos.csv'
path_db = 'sqlite:///db/filmes.db'

engine = create_engine(path_db)

df_test = pd.read_csv(path_csv)

def test_num_registros():
    num_registros_csv = len(df_test)

    with engine.connect() as cnn:
        result = cnn.execute(text("SELECT COUNT(*) FROM filmes"))
        num_registros_db = result.scalar()

    assert num_registros_csv == num_registros_db, f'Quantidade em CSV {num_registros_csv} é diferente da quantidade no banco {num_registros_db}'

def test_column_nao_vazia():
    
    df_db = pd.read_sql("SELECT * FROM filmes", con=engine)
    
    colunas_criticas = ['title', 'overview', 'budget', 'genres', 'revenue', 'runtime', 'release_date']
    
    for coluna in colunas_criticas:
        total_nan_col = df_db[coluna].isna().sum()
        total_row_df = len(df_db)
        assert total_nan_col < total_row_df, f'A coluna {coluna} está vazia'

def test_column_numericas():
    
    df_db = pd.read_sql("SELECT * FROM filmes", con=engine)
    
    colunas_numericas = ['id', 'budget', 'revenue', 'runtime', 'popularity', 'vote_average', 'vote_count']
    
    for coluna in colunas_numericas:
        
        assert df_test[coluna].dtype == df_db[coluna].dtype, f'A coluna {coluna} está com tipagem incorreta'