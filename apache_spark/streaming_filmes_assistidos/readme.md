# Spark

## Pipeline de ETL no Databricks

Pipeline de ETL no ambiente Databrisk para dados de uma plataforma de streaming de filmes.

- Ingestão: leitura de arquivos CSV raw.
- Transformações: limpeza, tratamento de tipos e padronização dos dados.
- Enriquecimento dos dados: relacionamento das bases na camada silver para formar uma base única base na camada gold.
- Performance: aplicação de caching para realização de análises e particionamento em formato parquet no resultado final para leitura mais rápida.

**Palavras-chave:** PySpark, Python, Databricks.
