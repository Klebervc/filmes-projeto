import pandas as pd
from sqlalchemy import create_engine, text
import os

path_csv = 'data/processed/filmes_limpos.csv'
path_db = 'db/filmes.db'

os.makedirs('db', exist_ok=True)

engine = create_engine(f'sqlite:///{path_db}')

df = pd.read_csv(path_csv)

df.to_sql('filmes', con=engine, if_exists='replace', index=False)

with engine.connect() as conn:
    result = conn.execute(text("PRAGMA table_info(filmes)"))

print("Esquema da tabela 'filmes':")
for row in result:
    print(f"Nome: {row[1]}, Tipo: {row[2]}")