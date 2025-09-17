# filmes-projeto

![](assets/Filme.jpg)

Neste repositório faremos um modelo de Machine Learning que usa filmes como features. O dataset primário usado para tratamento pode ser acessado em:

- [Dataset](./data/raw/movies_dataset.csv)

O dataset tratado encontra-se em:

- [Dataset_Limpo](./data/processed/filmes_limpos.csv)

Fizemos o tratamento do dataset bruto `movies_dataset.csv` onde corrigimos a tipagem das colunas, fizemos a limpeza de dados ausentes (`NaN`), removemos dados duplicados e exploramos os dados com alteração do dataset deixando apenas dados mais relevantes para o modelo de Machine Leaning. A descrição detalhada do que foi feito pode ser encontrada no notebook:

- [Notebook](./notebooks/01_exploracao_e_limpeza_de_dados.ipynb)