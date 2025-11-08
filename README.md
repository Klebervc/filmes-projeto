# filmes-projeto

![](assets/Filme.jpg)

Neste repositório faremos um modelo de Machine Learning que usa filmes como features. O dataset primário usado para tratamento pode ser acessado em:

[Dataset](data/raw/movies_dataset.csv)

O dataset tratado encontra-se em:

[Dataset_Limpo](data/processed/filmes_limpos.csv)

Fizemos o tratamento do dataset bruto `movies_dataset.csv` onde corrigimos a tipagem das colunas, fizemos a limpeza de dados ausentes __(NaN)__, removemos dados duplicados, exploramos os dados com alteração do dataset preechendo alguns registros ausentes e deixando apenas dados mais relevantes para o modelo de Machine Leaning. A descrição detalhada do que foi feito, pode ser encontrada no notebook:

[Notebook](notebooks/01_exploracao_e_limpeza_de_dados.ipynb)

Para fazermos consultas em __SQL__ com __Python__, criamos o __script__ `db_ingest.py` que primeiramente verifica se o diretório `db/` está criado (onde será criado a __base de dados__ `filmes.db`) e, em caso negativo cria o diretório. Partindo disso, carrega o __DataFrame__ e cria a __engine__ de conexão com a base de dados que será usada para criar a `filmes.db` juntamente com a tabela `filmes` que possue as mesmas colunas e linhas do DataFrame. Fizemos todo o código considerando os possíveis erros que podem ocorrer durante a execução. Além disso, o script diz a __tipagem__ de todas as colunas da tabela `filmes`.

Fizemos, também, o script de testes. Estes, verificam se o número de registros da tabela `filmes` é igual ao número de registros do __CSV__, verificam se nenhuma coluna da tabela `filmes` é __vazia__ e se a tipagem de todas as colunas do DataFrame é igual as da tabela `filmes`. Além disso, também fizemos os testes do arquivo `db_ingest.py` testando a criação do diretório `db`, criação da `engine` e tabela `filmes`, comparar o DataFrame e DataBase criados e testar o reconhecimento de `prints`. Criamos, também, o arquivo `test_db_ingest_except.py` para testar todos os `excepts` dos blocos `try / except` do código principal.

Por fim, realizamos diversos tipos de treinamento dos dados em modelos de Machine Learn tanto de Regressão quanto de Classificação que estão separados abaixo por seções:

## Ajuste de dados

Pode ser encontrado em:

[Notebook](ml/exercicios_ajustes_dados_random_forest.ipynb)

Nesse script, o modelo aplicado foi de Floresta Aleatória para inputar a coluna `budget` do dataframe com a métrica sendo a curva ROC_AUC. Os insights podem ser encontrados em 

[Relatório](ml/relatorio_ml.md)

## Regressão Linear

Pode ser encontrado em:

[Notebook](ml/exercicios_regressao_linear.ipynb)

Nesse script, inputamos a coluna `revenue` com regressão linear mas anteriormente a isso fizemos uma avaliação do modelo por MSE manual, MSE dado pelo `scikit learning` e `R2`. Os insights podem ser encontrados em

[Relatório](ml/relatorio_ml.md)

## Regressão Logística

Pode ser encontrado em:

[Notebook](ml/exercicios_regressao_logistica.ipynb)

Nesse script, criamos a variável binária `sucesso` para fazer previsões sobre ela a partir das features `budget`, `popularity` e `avaliacao_media`. A métrica utilizada foi a curva `ROC_AUC` e interpretamos os `coeficientes log-odds` do modelo. Os insights podem ser encontrados em

[Relatório](ml/relatorio_ml.md)

## Random Forest

Pode ser encontrado em:

[Notebook](ml/exercicios_random_forest.ipynb)

Nesse escript, treinamos os modelos de Arvore de Decisão, Regressão Logística, Floresta Aleatória e XGBoost a fim de prever `sucesso`. As principal métrica utilizada foi a `ROC_AUC`. Os insights podem ser encontrados em

[Relatório](ml/relatorio_ml.md)

## k-Nearest Neighbors (kNN)

Pode ser encontrado em:

[Notebook](ml/exercicios_knn.ipynb)

Nesse script, treinamos um modelo de KNN para prever `sucesso` em um range de `k vizinhos` variando de 1 a 20. A métrica utilizada é a acurácia média dada pelo cross validation. Os insights podem ser encontrados em

[Relatório](ml/relatorio_ml.md)

## Naive Bayes (GaussianNB)

Pode ser encontrado em:

[Notebook](ml/exercicios_naive_bayes.ipynb)

Nesse script, treinamos um modelo de Naive Bayes, mais precisamente uma GaussianaNB para prever `sucesso` comparando os treinamentos manualmente e com o uso do scikit-learn através da métrica `f1_score`. Os insights podem ser encontrados em

[Relatório](ml/relatorio_ml.md)

## Regressão Ridge e Lasso com validação Cruzada e Comparação de Modelos

Pode ser encontrado em:

[Notebook](ml/exercicios_regressao_ridge_lasso.ipynb)

Nesse script, treinamos com k-folds fazendo o grid search modelos de regressão linear, Ridge e Lasso para prever `revenue` e vermos os efeitos da regularização (Ridge e Lasso) no modelo no que diz respeito também a overfitting. As méstricas utilizadas foram `RMSE` e `R2`. Os insights podem ser encontrados em

[Relatório](ml/relatorio_ml.md)

## K-Means

Pode ser encontrado em:

[Notebook](ml/exercicios_k_means.ipynb)

Nesse script, treinamos um modelo de K-Means para agrupar o dataset segundo as features `popularity`, `vote_average` e `budget` encontrando o valor ideal para a métrica `silhouette score` a fim de separar o dataset em `clusters` bem definidos. Os insights podem ser encontrados em

[Relatório](ml/relatorio_ml.md) 