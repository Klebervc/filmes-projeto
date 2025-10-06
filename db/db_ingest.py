import pandas as pd
from sqlalchemy import create_engine, text
import os

def processar_filmes(path_csv='data/processed/filmes_limpos.csv', path_db='db/filmes.db'):
    
    os.makedirs(os.path.dirname(path_db), exist_ok=True)
    
    engine = create_engine('sqlite:///{path_db}')
    
    df = pd.read_csv(path_csv)
    
    df.to_sql('filmes', con=engine, if_exists='replace', index=False)
    
    with engine.connect() as cnn:
        result = cnn.execute(text("PRAGMA table_info(filmes)"))
    
    print("Informações da tabela 'filmes':")
    for row in result:
        print(f"Nome: {row[1]}, Tipo: {row[2]}")
    
    return engine

processar_filmes()