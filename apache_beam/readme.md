## Pipeline de Dados com Apache Beam: Casos de Dengue x Precipitação

Pipeline de dados em batch, desenvolvido em Python com Apache Beam, que integra dois datasets independentes (casos de dengue e volume de chuvas) em uma única base consolidada por estado e mês.

- Ingestão e parsing: leitura dos arquivos brutos (TXT delimitado por | e CSV), conversão de cada linha em dicionário com os nomes das colunas.
- Pipeline de dengue: criação da chave ano_mes a partir da data de início da semana epidemiológica, agrupamento por UF (GroupByKey) e soma dos casos por UF + ano_mes (FlatMap + CombinePerKey).
- Pipeline de chuvas: montagem da chave UF + ano_mes, tratamento de valores inválidos (negativos e medições com falha, como -9999, são descartados) e soma da precipitação mensal, arredondada para 1 casa decimal.
- Integração dos datasets: junção das duas PCollections por chave com CoGroupByKey, removendo registros em que faltava informação de chuva ou de dengue.
- Carga: exportação do resultado em CSV (uf; ano; mes; precipitacao_mm; casos_dengue) e validação da saída com pandas.

**Palavras-chave:** Apache Beam, Python, Pandas, regex, Processamento em batch, ETL.
