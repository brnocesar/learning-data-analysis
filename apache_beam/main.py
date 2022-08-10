import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText

file_path_dengue = '../datasets/apache_beam_casos_dengue.txt'

#%%
opts     = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=opts)

# pcollection: recebe o resultado de todos os processos/passos aplicados na pipeline
dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText(file_path_dengue, skip_header_lines=1) # passo 1: leitura do arquivo, recupera cada linha como uma string
    | "Mostrar resultados" >> beam.Map(print)
)

pipeline.run()
