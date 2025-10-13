import numpy as np
import plotly.graph_objects as go
from dash import dcc, html
import dash_bootstrap_components as dbc


def linha(x1, y1, x2, y2, n=100):
    return np.linspace(x1, x2, n), np.linspace(y1, y2, n)

def elipse(cx, cy, rx, ry, n=200):
    t = np.linspace(0, 2 * np.pi, n)
    return cx + rx * np.cos(t), cy + ry * np.sin(t)

# primeiro "4"
x4_vdir, y4_vdir = linha(2, 0, 2, 3)
x4_h, y4_h       = linha(0.5, 1.5, 2.3, 1.5)
x4_diag, y4_diag = linha(0.5, 1.5, 1.5, 3)

# "0"
x0, y0 = elipse(3.5, 1.5, 0.8, 1.5)

# segundo "4"
x4b_vdir, y4b_vdir = linha(6.2, 0, 6.2, 3)
x4b_h, y4b_h       = linha(4.7, 1.5, 6.5, 1.5)
x4b_diag, y4b_diag = linha(4.7, 1.5, 5.7, 3)

# cria figura
fig = go.Figure()

# adiciona todas as curvas
for x, y in [
    (x4_vdir, y4_vdir), (x4_h, y4_h), (x4_diag, y4_diag),
    (x0, y0),
    (x4b_vdir, y4b_vdir), (x4b_h, y4b_h), (x4b_diag, y4b_diag)
]:
    fig.add_trace(go.Scatter(x=x, y=y, mode='lines', line={'color': '#153d58', 'width': 5}))

fig.update_layout(
    title={
        'text': "ERRO",
        'font': {'size': 36, 'color': '#153d58'},
        'xanchor': 'center',
        'x': 0.5,
        'yanchor': 'top',
        'y': 0.9
    },
    xaxis={'autorange': False, 'showgrid': False, 'range': [0, 7.1], 'tickvals': [0, 1, 2, 3, 4, 5, 6, 7]},
    yaxis={'autorange': False, 'showgrid': False, 'range': [-0.1, 3.1], 'tickvals': [1, 2, 3]},
    width=900, 
    height=500,
    showlegend=False,
    plot_bgcolor='white',
    shapes=[
        {
            'type': "line",
            'x0': 0, 'y0': -0.1,
            'x1': 0, 'y1': 3.1,
            'line': { 'color': '#153d58', 'width': 2.4 },
        }, # eixo vertical (borda esquerda)
        {
            'type': "line",
            'x0': 0, 'y0': -0.1,
            'x1': 7.1, 'y1': -0.1,
            'line': { 'color': '#153d58', 'width': 2.4 },
        } # eixo horizontal (borda superior)
    ]
)

curva_404 = html.Div([dcc.Graph(figure=fig)])


layout = html.Div([
    html.H1('Página não encontrada', className='text-center mt-5 mb-5 custom-title'),
    dbc.Container([
        dbc.Row([
            dbc.Col([curva_404], md=12, className="d-flex justify-content-center")
        ])
    ])
])
