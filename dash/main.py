from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pages
from app import app

navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Home", href="/")),
        dbc.NavItem(dbc.NavLink("Gráficos", href="/graficos")),
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem("Formulários", header=True),
                dbc.DropdownMenuItem("Doença Cardíaca", href="/formularios/doenca_cardiaca"),
                dbc.DropdownMenuItem("Preço de Venda de uma Casa", href="/formularios/preco_casa"),
                dbc.DropdownMenuItem("Page 3", href="#"),
            ],
            nav=True,
            in_navbar=True,
            label="Modelos de Previsão",
        ),
    ],
    brand="brnocesar",
    brand_href="https://github.com/brnocesar",
    color="primary",
    dark=True,
)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    navbar,
    html.Div(id='conteudo')
])

@app.callback(
    Output('conteudo', 'children'),
    [Input('url','pathname')]
)
def mostrar_pagina(pathname):
    if pathname == '/':
        return pages.home.layout
    elif pathname == '/formularios/doenca_cardiaca':
        return pages.doenca_cardiaca.layout
    elif pathname == '/formularios/preco_casa':
        return pages.preco_casa.layout
    elif pathname == '/graficos':
        return pages.graficos.layout
    else:
        return pages.erro_404.layout

app.run(debug=True) # ambiente de desenvolvimento
# app.run(debug=False, port=8080, host='0.0.0.0') # ambiente de producao
