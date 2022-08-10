import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
import re

file_path_dengue = '../datasets/apache_beam_casos_dengue.txt'

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

def generate_dengue_cases(element):
    """
    Recebe 1 (uma) tupla no formato ('UF', [{}, {}, ...]), onde o segundo elemento 
    é uma lista com N dicionários. 
    Retorna N tuplas no formato ('UF-ano_mes', m), onde 'ano_mes' é o hash criado 
    anteriormente sobre o inicio da semana epidemiológica e m seu respectivo número de casos.
    """
    uf, rows = element
    for row in rows:
        num_casos = row['casos'].replace(',', '.')
        yield (f"{uf}-{row['ano_mes']}", float(num_casos) if re.search("^(\d+)([.]\d+)?$", num_casos) else 0.0)

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
    | "Soma casos de dengue pela chave 'UF e ano_mes'" >> beam.CombinePerKey(sum) # passo 8
    | "Mostrar resultados" >> beam.Map(print)
)

pipeline.run()
