import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
import re

file_path_dengue = '../datasets/apache_beam_casos_dengue.txt'
file_path_chuvas = '../datasets/apache_beam_chuvas.csv'

#%%
# passa uma linha (string) para lista, onde cada coluna se torna um item
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
    | "De lista para dicionário" >> beam.Map(list_to_dict, labels_dengue) # passo 3: monta um dicionário com os elementos da lista
    | "Trata data no arquivo de dengue" >> beam.Map(create_hash_from_date) # passo 4: cria hash para data de inicio da semana epidemiologica
    | "Cria chave com valor da UF" >> beam.Map(create_key_uf) # passo 5
    | "Agrupa por UF" >> beam.GroupByKey() # passo 6
    | "Descompactar casos de dengue" >> beam.FlatMap(generate_dengue_cases) # passo 7: descompacta os dicionários agrupados por UF, adiciona o campo 'ano_mes' na chave e mantém apenas o número de casos no elemento
    | "Soma casos de dengue pela chave 'UF e ano_mes'" >> beam.CombinePerKey(sum) # passo 8: a função passada para o CombinePerKey() recebe TODOS os elementos que compartilham a chave
#    | "Mostrar resultados de dengue" >> beam.Map(print)
)

pipeline.run()

#%%
chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> ReadFromText(file_path_chuvas, skip_header_lines=1) # passo 1: leitura do arquivo, recupera cada linha como uma string
    | "De texto para lista (chuvas)" >> beam.Map(row_to_list, sep=',') # passo 2: separa as colunas na string em uma lista
    | "Cria chave UF-aaaa-mm" >> beam.Map(create_key_uf_ano_mes_from_list) # passo 3: o resultado são elementos com uma chave de UF+ano_mes e a respectiva quantidade de precipitação (já descartando as medidas erradas)
    | "Soma quantidade de chuva por 'UF e ano_mes'" >> beam.CombinePerKey(sum) # passo 4: a função passada para o CombinePerKey() recebe TODOS os elementos que compartilham a chave
    | "Arredonda valor total da precipitação" >> beam.Map(round_value) # passo 5
#    | "Mostrar resultados de chuvas" >> beam.Map(print) # não posso repetir o nome/identificador dentro de uma pipeline (mas tava funcionando sem reclamar)
)

pipeline.run()

#%%
resultados = (
    (chuvas, dengue) # passo como primeiro parâmetro não mais a pipeline, mas as duas pcollections que quero unir
    | "Empilha pcollections" >> beam.Flatten() # passo 1: empilha todas as pcollections passadas como parâmetro
    | "Agrupa pcollections pela chave" >> beam.GroupByKey() # passo 2
    | "Mostrar resultados empilhados" >> beam.Map(print)
)

pipeline.run()
