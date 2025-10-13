from db_ingest import processar_filmes

path_csv = 'data/processed/filmes_limpos.csv'
path_db = 'db/filmes.db'

processar_filmes(path_csv, path_db)

