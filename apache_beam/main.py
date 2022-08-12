import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
import pandas as pd
import re
import time

file_path_dengue = '../datasets/apache_beam_casos_dengue.txt'
file_path_chuvas = '../datasets/apache_beam_chuvas.csv'
file_path_chuvas = '../datasets/apache_beam_chuvas_mini_sample.csv'

#%%
# passa uma linha do arquivo (string) para lista, onde cada coluna se torna um item da lista
row_to_list  = lambda row, sep='|': row.split(sep)

# passa uma lista para dicionário, onde cada item recebe como rótulo sua respectiva coluna
list_to_dict = lambda values, labels: dict(zip(labels, values))

def create_hash_from_date(row, label='data_iniSE'):
    """
    Afim de unificar o dataset de 'casos de dengue' com o de 'chuvas', devemos 
    criar um hash (campo 'ano_mes') para o campo com a data de início da semana 
    epidemiológica ('data_iniSE').
    """
    row['ano_mes'] = '-'.join(row[label].split('-')[:2])
    return row

def create_hash_from_date_regex(row):
    """
    Afim de unificar o dataset de 'casos de dengue' com o de 'chuvas', devemos 
    modificar o campo com a data de início da semana epidemiológica.
    Remove campo 'data_iniSE' e cria o campo 'ano_mes'.
    """
    p              = re.compile("^(19\d{2}|20([01]\d|2[0-2]))-(0\d|1[0-2])")
    result         = p.search(row.pop('data_iniSE'))
    row['ano_mes'] = result.group() if result else None
    return row

# cria chave com valor de UF, para que seja possível agrupar as linhas (elementos do pipeline(?)) por UF
create_key_uf = lambda row: (row['uf'], row)

def validate_float_value(value):
    """
    Verifica se value é float POSITIVO: se for retorna o casting para float, do contrário retorna zero.
    
    Obs.: Por "extrema perícia" do programador, a função já está avaliando se o 
    número é negativo e barrando-o. O que foi totalmente proposital, já que outras 
    situações que demandariam tratamento similar, como por exemplo as medidas com valor -9999.
    """
    value = str(value).replace(',', '.')
    return float(value) if re.search("^(\d+)([.]\d+)?$", value) else 0.0

def validate_int_value(value, allow_negative=False, return_original=True):
    """
    Verifica se value é um int: se for retorna o casting para int, do contrário retorna zero ou o valor original.
    """
    minus_signal = '(-)?' if allow_negative else ''
    fail_return  = value if return_original else 0
    return int(value) if re.search(f"^{minus_signal}(\d+)$", value) else fail_return

def generate_dengue_cases(element):
    """
    Recebe 1 (uma) tupla no formato ('UF', [{}, {}, ...]), onde o segundo elemento 
    é uma lista com N dicionários. 
    Retorna N tuplas no formato ('UF-ano_mes', m), onde 'ano_mes' é o hash criado 
    anteriormente sobre o inicio da semana epidemiológica e m seu respectivo número de casos.
    """
    uf, rows = element
    for row in rows:
        yield (f"{uf}-{row['ano_mes']}", validate_float_value(row['casos']))

# transforma lista [data, n, uf] em tupla com chave e valor (uf-data_hash, n)
create_key_uf_ano_mes_from_list = lambda element: (f"{element[2]}-{'-'.join(element[0].split('-')[:2])}", validate_float_value(element[1]))

# recebe tupla com chave e valor, retorna mesma tupla mas com valor arredondado
round_value = lambda element: (element[0], round(element[1], 1))

# retorna o resultado (bool) da avaliação se o elemento possui todos os valores (True) ou algum deles faltando (False)
filter_empty_values = lambda element: all( [element[1]['chuvas'], element[1]['dengue']] )

# recebe uma tupla no formato (<chave>, <dicionário com os valores contabilizados>) e retorna uma tupla com todos valores descompactados
explode_values_to_tuple = lambda element: tuple([validate_int_value(i) for i in element[0].split('-')] + [element[1]['chuvas'][0], element[1]['dengue'][0]])

# recebe uma tupla no formato (<chave>, <dicionário com os valores contabilizados>) e retorna uma string com todos os valores concatenados
explode_values_to_string_for_csv = lambda element, sep=';': sep.join(element[0].split('-') + [str(element[1]['chuvas'][0]), str(element[1]['dengue'][0])])

# recebe uma tupla e retorna uma string para ser enviada a um CSV, com todos os itens concatenados
tuple_to_string_for_csv = lambda element, sep=';': sep.join([str(i) for i in element])

#%%
with open(file_path_dengue, "r") as file:
    first_line = file.readline().replace('\n', '')
    file.close()

# lista com os nomes das colunas no arquivo
labels_dengue = row_to_list(first_line)

#%%
opts     = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=opts)

# pcollection: recebe o resultado de todos os processos/passos aplicados na pipeline
dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText(file_path_dengue, skip_header_lines=1) # passo 1: leitura do arquivo, recupera cada linha como uma string
    | "De texto para lista" >> beam.Map(row_to_list) # passo 2: separa as colunas na string em uma lista
    | "De lista para dicionário" >> beam.Map(list_to_dict, labels_dengue) # passo 3: monta um dicionário com os elementos da lista como valores e as respectivas colunas como chaves 
    | "Trata data no arquivo de dengue" >> beam.Map(create_hash_from_date) # passo 4: cria hash para data de inicio da semana epidemiologica
    | "Cria chave com valor da UF" >> beam.Map(create_key_uf) # passo 5: elemento passa a ser uma tupla, primeiro item sendo a chave e o segundo o dicionário com os dados
    | "Agrupa por UF" >> beam.GroupByKey() # passo 6: elemento passa a ser uma tupla, primeiro item sendo a chave e o segundo uma lista com os dicionários com chave em comum
    | "Descompactar casos de dengue" >> beam.FlatMap(generate_dengue_cases) # passo 7: descompacta os dicionários agrupados por UF, adiciona o campo 'ano_mes' na chave e mantém apenas o número de casos no elemento
    | "Soma casos de dengue pela chave 'UF e ano_mes'" >> beam.CombinePerKey(sum) # passo 8: a função passada para o CombinePerKey() agrupa os elementos que compartilham a chave
#    | "Mostrar resultados de dengue" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> ReadFromText(file_path_chuvas, skip_header_lines=1) # passo 1
    | "De texto para lista (chuvas)" >> beam.Map(row_to_list, sep=',') # passo 2: separa as colunas na string em uma lista
    | "Cria chave UF-aaaa-mm" >> beam.Map(create_key_uf_ano_mes_from_list) # passo 3: o resultado são elementos com uma chave de UF+ano_mes e a respectiva quantidade de precipitação (já descartando as medidas erradas)
    | "Soma quantidade de chuva por 'UF e ano_mes'" >> beam.CombinePerKey(sum) # passo 4: a função passada para o CombinePerKey() recebe TODOS os elementos que compartilham a chave
    | "Arredonda valor total da precipitação" >> beam.Map(round_value) # passo 5
#    | "Mostrar resultados de chuvas" >> beam.Map(print) # não posso repetir o nome/identificador dentro de uma pipeline (mas tava funcionando sem reclamar)
)

resultados = (
    ({'chuvas': chuvas, 'dengue': dengue}) # passo como primeiro parâmetro não mais a pipeline, mas as duas pcollections que quero unir. Agora passo como dicionário para que seja possível identificar os valores de cada uma
    | "Mescla pcollections" >> beam.CoGroupByKey() # passo 1: junta as pcollections passadas como parâmetro e já agrupa pela chave
    | "Filtrar dados vazios" >> beam.Filter(filter_empty_values) # passo 2: deixa apenas elementos que possuem os dois valores. O método beam.Filter() remove os elementos que retornarem False
    | "Descompactar elementos" >> beam.Map(explode_values_to_tuple) # passo 3: transforma os elementos em tuplas em que cada item é um dos valores pertinentes
    | "Prepara elementos para CSV" >> beam.Map(tuple_to_string_for_csv) # passo 4: concatena todos os itens do elemento em uma string separada por ';'
    | "Mostrar resultados empilhados" >> beam.Map(print)
)

export_file_name = f"resultado_{int(time.time())}"
delimitador      = ';'
header_csv       = delimitador.join(['uf', 'ano', 'mes', 'precipitacao_mm', 'casos_dengue'])

resultados | "Criar arquivo CSV" >> WriteToText(export_file_name, file_name_suffix='.csv', shard_name_template='', header=header_csv)

pipeline.run()

#%%
df_resultado = pd.read_csv(f"{export_file_name}.csv", sep=delimitador)
print("Dimensão do df: ", df_resultado.shape)
print(df_resultado.head())
