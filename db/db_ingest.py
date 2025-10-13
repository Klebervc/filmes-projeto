import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os

def processar_filmes(path_csv, path_db):
    
    try:
        os.makedirs(os.path.dirname(path_db), exist_ok=True)    
    except OSError as dir_error:
        print(f'Erro na criação do diretório da base de dados: {dir_error}')
        return
    
    try:    
        engine = create_engine(f'sqlite:///{path_db}')
    except SQLAlchemyError as engine_error:
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
    except OSError as os_error:
        print(f'Erro ao acessar arquivo CSV: {os_error}')
        return
    except Exception as variant_error:
        print(f'Erro inesperado ao carregar CSV: {variant_error}')
        return
    
    erros = []
    
    for idx, row in df.iterrows():
        
        try:
            row_df = row.to_frame().T
            row_df.to_sql('filmes', con=engine, if_exists='append', index=False)
        
        except ValueError as value_error:
            erro_dict = {
                'index': idx,
                'linha': row.to_dict(),
                'erro': str(value_error)
            }
            
            erros.append(erro_dict)
            continue
        except SQLAlchemyError as db_tabela_error:
            erros.append(db_tabela_error)
            return erros
        
    if erros:
        print('Erros encontrados na validação')
        for erro in erros:
            print(erro)
    else:
        df.to_sql('filmes',con=engine, if_exists='replace', index=False)
    
    try:
        with engine.connect() as cnn:
            result = cnn.execute(text("PRAGMA table_info(filmes)"))
    
        print("Informações da tabela 'filmes':")
        for row in result:
            print(f"Nome: {row[1]}, Tipo: {row[2]}")
    except SQLAlchemyError as consulta_SQL:
        print(f'Erro ao realizar consulta SQL: {consulta_SQL}')
        return
    
    return {'erros': erros, 'engine': engine}

