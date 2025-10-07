import pandas as pd
from sqlalchemy import create_engine, text
import os

def processar_filmes(path_csv='data/processed/filmes_limpos.csv', path_db='db/filmes.db'):
    
    try:
        os.makedirs(os.path.dirname(path_db), exist_ok=True)    
    except Exception as dir_error:
        print(f'Erro na criação do diretório da base de dados: {dir_error}')
        return
    
    try:    
        engine = create_engine(f'sqlite:///{path_db}')
    except Exception as engine_error:
        print(f'Erro na criação da engine: {engine_error}')
        return
    
    try:
        df = pd.read_csv(path_csv)
    except FileNotFoundError:
        print(f'O arquivo CSV não existe: {path_csv}')
        return
    except pd.errors.ParserError as read_error:
        print(f'Erro ao ler o arquivo CSV: {read_error}')
        return
    except Exception as variant_error:
        print(f'Erro ao carregar CSV: {variant_error}')
        return
    
    try:
        df.to_sql('filmes', con=engine, if_exists='replace', index=False)
    except Exception as salvar_tabela:
        print(f'Erro ao salvar tabela: {salvar_tabela}')
        return
    
    try:
        with engine.connect() as cnn:
            result = cnn.execute(text("PRAGMA table_info(filmes)"))
    
        print("Informações da tabela 'filmes':")
        for row in result:
            print(f"Nome: {row[1]}, Tipo: {row[2]}")
    except Exception as consulta_SQL:
        print(f'Erro ao realizar consulta SQL: {consulta_SQL}')
        return
    
    return engine

processar_filmes()