import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html
import dash_bootstrap_components as dbc

# carrega dados
df = pd.read_csv('datasets/formacao_ds_2_data_visualization_imigrantes_canada.csv')
df_america_sul = df.set_index('País').query(" Região == 'América do Sul' ").drop(columns=['Continente', 'Região', 'Total']).T.reset_index().rename(columns={'index': 'ano'})
df_america_sul['ano'] = df_america_sul['ano'].astype(int)


# cria figura
fig = go.Figure()

# define uma linha para cada pais, adicionando apenas o primeiro ponto
coluna_x = 'ano'
for pais in df_america_sul.columns:
    if pais == coluna_x:
        continue
    fig.add_trace(
        go.Scatter(x=[df_america_sul[coluna_x][0]], y=[df_america_sul[pais].iloc[0]], mode='lines', name=pais, line={'width':2})
    )

# define configuracoes do layout
y_max = int((df_america_sul.values.max() * 1.1)/1000)*1000
fig.update_layout(
    # title={
    #     'text': 'Gráfico de linhas animado',
    #     'x': 0.12,
    #     'xanchor': 'left',
    #     'font': {'size':20}
    # },
    xaxis={'range': [1980, 2013], 'autorange':False, 'title': '<b>Ano</b>'},
    yaxis={'range': [0, y_max], 'autorange':False, 'title': '<b>Número de imigrantes</b>', 'tickvals': [i for i in range(500, y_max, 500)]},
    updatemenus=[{
        'type': 'buttons',
        'showactive': False,
        'xanchor': 'left', 'y': -0.04, 'x': 1.015,
        'buttons': [{
            'label': 'Plotar gráfico',
            'method': 'animate',
            'args': [None, {'frame': {'duration': 100, 'redraw': True}, 'fromcurrent': True}]
        }]
    }],
    width=1400, height=700, showlegend=True
)

# define os frames da animacao
frames = []
for i in range(len(df_america_sul)):
    data = []
    for pais in df_america_sul.columns:
        if pais == coluna_x:
            continue
        data.append(go.Scatter(x=df_america_sul[coluna_x][:i+1], y=df_america_sul[pais].iloc[:i+1]))
    frames.append(go.Frame(data=data))
fig.frames = frames

grafico_animado = html.Div([dcc.Graph(figure=fig)])


layout = html.Div([
    html.H1('Imigração da América do Sul para o Canadá - 1980 a 2013', className='text-center mt-5 mb-5 custom-title'),
    dbc.Container([
        dbc.Row([
            dbc.Col([grafico_animado], md=12, className="d-flex justify-content-center")
        ])
    ])
])
