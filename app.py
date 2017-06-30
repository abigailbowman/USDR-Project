# -*- coding: utf-8 -*-
import dash
import dash_html_components as html
import dash_core_components as dcc

import pandas as pd

import usdr

accts = usdr.loadUSDR()

def generate_table(dataframe, max_rows=25):
    if type(dataframe) == pd.core.series.Series:
        dataframe = dataframe.to_frame()
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))
    ], className='resultsTable')

app = dash.Dash()

app.layout = html.Div([
    html.A([ 'Print PDF' ],
        className="button no-print"),
           
#==============================================================================
#     html.Label('Dropdown'),
#     dcc.Dropdown(
#         options=[
#             {'label': 'New York City', 'value': 'NYC'},
#             {'label': u'Montréal', 'value': 'MTL'},
#             {'label': 'San Francisco', 'value': 'SF'}
#         ],
#         value='MTL'
#     ),
# 
#     html.Label('Multi-Select Dropdown'),
#     dcc.Dropdown(
#         options=[
#             {'label': 'New York City', 'value': 'NYC'},
#             {'label': u'Montréal', 'value': 'MTL'},
#             {'label': 'San Francisco', 'value': 'SF'}
#         ],
#         value=['MTL', 'SF'],
#         multi=True
#     ),
# 
#     html.Label('Radio Items'),
#     dcc.RadioItems(
#         options=[
#             {'label': 'New York City', 'value': 'NYC'},
#             {'label': u'Montréal', 'value': 'MTL'},
#             {'label': 'San Francisco', 'value': 'SF'}
#         ],
#         value='MTL'
#     ),
# 
#     html.Label('Checkboxes'),
#     dcc.Checklist(
#         options=[
#             {'label': 'New York City', 'value': 'NYC'},
#             {'label': u'Montréal', 'value': 'MTL'},
#             {'label': 'San Francisco', 'value': 'SF'}
#         ],
#         values=['MTL', 'SF']
#     ),
# 
#     html.Label('Text Input'),
#     dcc.Input(value='MTL', type='text'),
# 
#     html.Label('Slider'),
#     dcc.Slider(
#         min=0,
#         max=9,
#         marks={i: 'Label {}'.format(i) if i == 1 else str(i) for i in range(1, 6)},
#         value=5,
#     ),
#             
#==============================================================================
    generate_table(accts),
    
], style={'columnCount': 2})


external_css = ["https://codepen.io/chriddyp/pen/bWLwgP.css",
                "https://cdn.datatables.net/1.10.15/css/jquery.dataTables.min.css"]

for css in external_css:
    app.css.append_css({ "external_url": css })

external_js = ["https://code.jquery.com/jquery-3.2.1.min.js",
        "https://cdn.datatables.net/1.10.15/js/jquery.dataTables.min.js"]

for js in external_js:
    app.scripts.append_script({ "external_url": js })


#==============================================================================
# @app.callback(
#     dash.dependencies.Output('output-a', 'children'),
#     [dash.dependencies.Input('dropdown-a', 'value')])
# def callback_a(dropdown_value):
#     return 'You\'ve selected "{}"'.format(dropdown_value)
#==============================================================================
    
if __name__ == '__main__':
    app.run_server(debug=True)