# Spark

## Análise de Dados Cadastrais com Apache Spark

Projeto de processamento de dados com PySpark, com o objetivo de explorar, limpar e analisar os dados cadastrais de empresas, estabelecimentos e sócios brasileiros a partir de arquivos CSV particionados da base de CNPJ.

- Ingestão: leitura de arquivos CSV particionados em DataFrames do Spark, com inferência de schema para os datasets de empresas, estabelecimentos e sócios.
- Transformações: renomeação de colunas, conversão de tipos de dados, tratamento de valores monetários e datas, além da preparação dos campos para análises posteriores.
- Análise/consulta: uso da API de DataFrames do PySpark para inspeção de registros, contagem de linhas e exploração inicial dos dados.
- Saída: geração de DataFrames processados e possibilidade de gravação em arquivos locais em formato CSV/Parquet para uso posterior.

**Palavras-chave:** Apache Spark, PySpark, Python.
