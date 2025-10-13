from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import joblib
import pandas as pd
from app import app

modelo   = joblib.load('artifacts/models/preco_casa_sm_OLS.pkl')


formulario = dbc.Container([
    # dbc.Container([
        html.P("Preencha os campos abaixo e clique em PREVER para rodar o modelo de previsão para preço de venda de uma casa", className='text-center mt-3 mb-5 custom-subtitle'),
        dbc.Row([
            dbc.Col([
                dbc.CardGroup([
                    dbc.Label("Área do primeiro andar (m²)"),
                    dbc.Input(id='area_primeiro_andar', type='number', placeholder='Digite a área do primeiro andar (m²)'),
                ], className='mb-4 custom-input'),
            ], md=3),
            dbc.Col([
                dbc.CardGroup([
                    dbc.Label("Existe segundo andar?"),
                    dbc.Select(
                        id='existe_segundo_andar', 
                        options=[
                            {'label': 'Não', 'value': '0'},
                            {'label': 'Sim', 'value': '1'}
                        ], 
                        placeholder='Selecione'
                    ),
                ], 
                className='mb-2 custom-select'),
            ], md=3),
            dbc.Col([
                dbc.CardGroup([
                    dbc.Label("Quantidade de banheiros"),
                    dbc.Input(id='num_banheiros', type='number', placeholder='Digite a quantidade de banheiros'),
                ], className='mb-4 custom-input'),
            ], md=3),
            dbc.Col([
                dbc.CardGroup([
                    dbc.Label("A cozinha é excelente?"),
                    dbc.Select(id='cozinha_excelente', placeholder='Selecione', options=[
                        {'label': 'Sim', 'value': '1'},
                        {'label': 'Não', 'value': '0'}
                    ]),
                ], className='mb-2 custom-select'),
            ], md=3),
        ]),
        dbc.Row([  
            dbc.CardGroup([
                dbc.Label(""),
                dbc.Button("Prever", id='botao-prever', color='primary', n_clicks=0, className='w-25 mt-4', style={'font-size': '1.1rem'})
            ], className='d-flex justify-content-center mb-3'),
        ])
    # ])
], fluid=True)

layout = html.Div([
    dbc.Container([
        html.H1("Previsão do preço de venda de uma casa", className='text-center mt-5 custom-title'),
        formulario,
        html.Div(id='previsao_preco_casa')
    ])
])

@app.callback(
    Output('previsao_preco_casa', 'children', allow_duplicate=True),
    [Input('botao-prever', 'n_clicks')],
    [
        State('area_primeiro_andar',  'value'),
        State('existe_segundo_andar', 'value'),
        State('num_banheiros',        'value'),
        State('cozinha_excelente',    'value')
    ]
    ,prevent_initial_call=True
)
def prever_doenca(n_click, area_primeiro_andar, existe_segundo_andar, num_banheiros, cozinha_excelente):
    # if n_click == 0:
    #     return ''
    
    # verifica se algum campo esta vazio
    if ( area_primeiro_andar in [None, ''] or existe_segundo_andar in [None, ''] or num_banheiros in [None, ''] or cozinha_excelente in [None, ''] ):
        return dbc.Alert("Preencha todos os campos antes de clicar no botão PREVER.", color='danger', className='text-center mt-3', style={'font-size': '1.3rem'})
    
    # monta df com os dados do formulario
    df_input = pd.DataFrame(
        data    = [[1,       area_primeiro_andar,   existe_segundo_andar,   num_banheiros,   cozinha_excelente]],
        columns = ['const', 'area_primeiro_andar', 'existe_segundo_andar', 'num_banheiros', 'cozinha_excelente']
    ).astype(int)

    # faz a previsao
    previsao        = modelo.predict(df_input)[0]
    valor_formatado = f"{previsao:,.2f}".replace(',', 'xx').replace('.', ',').replace('xx', '.')
    
    return dbc.Alert(f"Previsão: R$ {valor_formatado}", color='success', className='text-center mt-3', style={'font-size': '1.3rem'})


# limpar resultado quando comecar digitar novamente
@app.callback(
    Output('previsao_preco_casa', 'children', allow_duplicate=True),
    [
        Input('area_primeiro_andar', 'value'),
        Input('existe_segundo_andar', 'value'),
        Input('num_banheiros', 'value'),
        Input('cozinha_excelente', 'value')
    ],
    prevent_initial_call=True
)
def aguardando_alert(*_):
    return dbc.Alert("Preenchendo formulário...", color='secondary', className='text-center mt-3', style={'font-size': '1.3rem'})