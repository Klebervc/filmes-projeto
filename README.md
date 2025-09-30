# filmes-projeto

![](assets/Filme.jpg)

Neste repositório faremos um modelo de Machine Learning que usa filmes como features. O dataset primário usado para tratamento pode ser acessado em:

[Dataset](data/raw/movies_dataset.csv)

O dataset tratado encontra-se em:

[Dataset_Limpo](data/processed/filmes_limpos.csv)

Fizemos o tratamento do dataset bruto `movies_dataset.csv` onde corrigimos a tipagem das colunas, fizemos a limpeza de dados ausentes __(NaN)__, removemos dados duplicados, exploramos os dados com alteração do dataset preechendo alguns registros ausentes e deixando apenas dados mais relevantes para o modelo de Machine Leaning. A descrição detalhada do que foi feito, pode ser encontrada no notebook:

[Notebook](notebooks/01_exploracao_e_limpeza_de_dados.ipynb)

Para fazermos consultas em __SQL__ com __Python__, criamos o __script__ `db_ingest.py` que primeiramente verifica se o diretório `db/` está criado (onde será criado a __base de dados__ `filmes.db`) e, em caso negativo cria o diretório. Partindo disso, carrega o __DataFrame__ e cria a __engine__ de conexão com a base de dados que será usada para criar a `filmes.db` juntamente com a tabela `filmes` que possue as mesmas colunas a linhas do DataFrame. Além disso, o script diz a __tipagem__ de todas as colunas da tabela `filmes`.

Fizemos, também, o script de testes. Estes, verificam se o número de registros da tabela `filmes` é igual ao número de registros do __CSV__, verificam se nenhuma coluna da tabela `filmes` é __vazia__ e se a tipagem de todas as colunas do DataFrame é igual as da tabela `filmes`. 