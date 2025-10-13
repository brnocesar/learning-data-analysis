from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import joblib
import pandas as pd
from app import app

modelo   = joblib.load('artifacts/models/doenca_cardiaca_xgboost.pkl')
medianas = joblib.load('artifacts/support/doenca_cardiaca_medianas.pkl')


formulario = dbc.Container([
    html.P("Preencha os campos abaixo e clique em PREVER para rodar o modelo de previsão para presença de doença cardíaca", className='text-center mt-3 mb-5 custom-subtitle'),
    dbc.Row([
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Idade"),
                dbc.Input(id='age', type='number', placeholder='Digite a idade')
            ], className='mb-3 custom-input'),
        ], md=2),
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Sexo Biológico"),
                dbc.Select(id='sex', options=[
                    {'label': 'Masculino', 'value': '1'},
                    {'label': 'Feminino',  'value': '0'}
                ])
            ], className='mb-3 custom-select'),
        ], md=2),
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Tipo de dor no peito"),
                dbc.Select(id='cp', options=[
                    {'label': 'Angina típica',        'value': '1'},
                    {'label': 'Angina atípica',       'value': '2'},
                    {'label': 'Não angina',           'value': '3'},
                    {'label': 'Angina assintomática', 'value': '4'}
                ])
            ], className='mb-3 custom-select'),
        ], md=3),
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Pressão arterial em repouso"),
                dbc.Input(id='trestbps', type='number', placeholder='Digite a pressão arterial em repouso'),
            ], className='mb-3 custom-input'),
        ], md=3),
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Colesterol sérico"),
                dbc.Input(id='chol', type='number', placeholder='Digite o colesterol sérico'),
            ], className='mb-3 custom-input'),
        ], md=2),
    ]),
    dbc.Row([
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Glicemia em jejum"),
                dbc.Select(id='fbs', options=[
                    {'label': 'Menor que 120 mg/dl', 'value': '0'},
                    {'label': 'Maior que 120 mg/dl', 'value': '1'}
                ]),
            ], className='mb-3 custom-select'),
        ], md=3),
        dbc.Col([
            dbc.CardGroup([
                    dbc.Label("Eletrocardiografia em repouso"),
                    dbc.Select(id='restecg', options=[
                        {'label': 'Normal',                           'value': '0'},
                        {'label': 'Anormalidades de ST-T',            'value': '1'},
                        {'label': 'Hipertrofia ventricular esquerda', 'value': '2'}
                    ]),
                ], className='mb-3 custom-select'),
        ], md=3),
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Frequência cardíaca máxima atingida"),
                dbc.Input(id='thalach', type='number', placeholder='Digite a frequência cardíaca máxima atingida'),
            ], className='mb-3 custom-input'),
        ], md=3),
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Angina induzida por exercício"),
                dbc.Select(id='exang', options=[
                    {'label': 'Sim', 'value': '1'},
                    {'label': 'Não', 'value': '0'}
                ]),
            ], className='mb-3 custom-select'),
        ], md=3),
    ]),
    dbc.Row([
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Depressão do segmento ST induzida por exercício"),
                dbc.Input(id='oldpeak', type='number', placeholder='Digite a depressão do segmento ST induzida por exercício'),
            ], className='mb-3 custom-input'),
        ], md=3),
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Inclinação do segmento ST"),
                dbc.Select(id='slope', options=[
                    {'label': 'Ascendente',  'value': '1'},
                    {'label': 'Plana',       'value': '2'},
                    {'label': 'Descendente', 'value': '3'}
                ]),
            ], className='mb-3 custom-select'),
        ], md=3),
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Número de vasos principais coloridos por fluoroscopia"),
                dbc.Select(id='ca',
                    options = [{'label': str(i), 'value': str(i)} for i in range(4)]
                )
            ], className='mb-3 custom-select'),
        ], md=3),
        dbc.Col([
            dbc.CardGroup([
                dbc.Label("Cintilografia do miocárdio"),
                dbc.Select(id='thal', options=[
                    {'label': 'Normal',             'value': '3'},
                    {'label': 'Defeito fixo',       'value': '6'},
                    {'label': 'Defeito reversível', 'value': '7'}
                ]),
            ], className='mb-3 custom-select'),
        ], md=3),
    ]),
    dbc.Row([  
        dbc.CardGroup([
            dbc.Label(""),
            dbc.Button("Prever", id='botao-prever', color='primary', n_clicks=0, className='w-25 mt-4', style={'font-size': '1.1rem'})
        ], className='d-flex justify-content-center mb-3'),
    ])
], fluid=True)

layout = html.Div([
    html.H1("Previsão de doença cardíaca", className='text-center mt-5 custom-title'),
    formulario,
    html.Div(id='previsao')
])

@app.callback(
    Output('previsao', 'children'),
    [Input('botao-prever', 'n_clicks')],
    [
        State('age',      'value'),
        State('sex',      'value'),
        State('cp',       'value'),
        State('trestbps', 'value'),
        State('chol',     'value'),
        State('fbs',      'value'),
        State('restecg',  'value'),
        State('thalach',  'value'),
        State('exang',    'value'),
        State('oldpeak',  'value'),
        State('slope',    'value'),
        State('ca',       'value'),
        State('thal',     'value')
    ]
)
def prever_doenca(n_click, age, sex, cp, trestbps, chol, fbs, restecg, thalach, exang, oldpeak, slope, ca, thal):
    if n_click == 0:
        return ''
    
    # monta df com os dados do formulario
    df_input = pd.DataFrame(
        data    = [[age,   sex,   cp,   trestbps,   chol,   fbs,   restecg,   thalach,   exang,   oldpeak,   slope,   ca,   thal]],
        columns = ['age', 'sex', 'cp', 'trestbps', 'chol', 'fbs', 'restecg', 'thalach', 'exang', 'oldpeak', 'slope', 'ca', 'thal']
    )

    df_input.fillna(medianas, inplace=True) # preenche NaN com medianas

    for col in df_input.columns:
        if df_input[col].dtype == 'object':
            # converte colunas para tipo numerico adequado
            df_input[col] = df_input[col].astype(float if col == 'oldpeak' else int)

    # faz a previsao
    previsao = modelo.predict(df_input)[0]

    # define a mensagem e nivel do alerta
    mensagem, cor_alerta = ("Doença cardíaca", "danger") if previsao == 1 else ("Sem doença cardíaca", "success")
    
    return dbc.Alert(f"Previsão: {mensagem}", color=cor_alerta, className='text-center mt-3', style={'font-size': '1.3rem'})


# limpar resultado quando começar digitar novamente
