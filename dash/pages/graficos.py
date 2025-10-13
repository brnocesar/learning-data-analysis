from ucimlrepo import fetch_ucirepo
import plotly.express as px
from dash import dcc, html
import dash_bootstrap_components as dbc

heart_disease   = fetch_ucirepo(id=45)
dados           = heart_disease.data.features
dados["doenca"] = (heart_disease.data.targets > 0) * 1

histogram_figure = px.histogram(dados, x="age", nbins=30, title="Distribuição etária")
histogram_div    = html.Div([dcc.Graph(figure=histogram_figure)])

boxplot_figure   = px.box(dados, x='doenca', y='age', color='doenca', title='Boxplot de idades')
boxplot_div      = html.Div([dcc.Graph(figure=boxplot_figure)])


layout = html.Div([
    html.H1('Análise de dados do UC Irvine Repository Heart Disease', className='text-center mt-5 mb-5 custom-title'),
    dbc.Container([
        dbc.Row([
            dbc.Col([histogram_div], md=8),
            dbc.Col([boxplot_div], md=4)
        ])
    ])
])
