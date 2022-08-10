import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText

file_path_dengue = '../datasets/apache_beam_casos_dengue.txt'

#%%
# passa uma linha (string) para lista, onde cada coluna se torna um item
row_to_list  = lambda row, sep='|': row.split(sep)

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
    | "Mostrar resultados" >> beam.Map(print)
)

pipeline.run()
