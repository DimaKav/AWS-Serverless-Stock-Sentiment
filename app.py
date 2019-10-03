import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.figure_factory as ff
import pandas as pd
import scipy

app = dash.Dash(__name__,
                external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server


"""Load the data"""
#---------------------------------------------#
url = "<your_api_url_here>"
df = pd.read_json(url).drop(['timestamp'],1)
#---------------------------------------------#

"""Utility functions"""
#---------------------------------------------#
# Get word counts for all data
def get_counts_all(data=df):
    data['string'] = [" ".join(i) for i in data['keywords']]
    return pd.Series(' '.join(data['string']).lower()\
                    .split()).value_counts()[:30]
# Get data for time sentiment
def get_time_sentiment(data=df):
    return df.groupby('publish_date').mean()
# Get each tickers sentiment into a single column by ticker
def columns_to_rows(data):
    dt = data[['ticker','sentiment']].set_index('ticker').T
    return dt.groupby(dt.columns.values, axis=1)\
           .agg(lambda x: x.values.tolist()).sum().apply(pd.Series).T
#---------------------------------------------#

"""Process the data"""
#---------------------------------------------#
df = df.reset_index(drop=True)
df = df.sort_values('publish_date', ascending=False)
df['sentiment'] = [round(i['compound'],2) for i in df['sentiment']]
df['publish_date'] = pd.to_datetime(df['publish_date'], utc=True)
df['publish_date'] = df['publish_date'].dt.date
#---------------------------------------------#


app.layout = html.Div([
    html.P('Data is updated every 3 days'),
    html.P(f'Total number of articles: {len(df)}'),
    html.P(f'Stocks: {df.ticker.unique()}'),
    html.Div([
        dash_table.DataTable(
            id='datatable',
            data=df.drop(['url'],1).to_dict('records'),
            columns=[
                {'id': i, 'name': i} for i in df.drop(['keywords','url'],1).columns
            ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'height': '500px',
                'overflowY': 'scroll',
                'border': 'thin lightgrey solid'
            },
            style_cell={'minWidth': '110px'},
            page_size= 7,
            filter_action='native',
            sort_action='native',
            style_data={'whiteSpace': 'normal'},
            selected_rows = []
        )
    ]),
    html.Div([
        dbc.Row([
            dbc.Col(
                html.Div(id='sentiment-distribution')
            ),
            dbc.Col([
                html.Div(id='sentiment-time')
            ]),
            dbc.Col(
                html.Div(id='keyword-counts')
            ),
        ]),
    ])
])

@app.callback(
    Output('keyword-counts', "children"),
    [Input('datatable', "derived_virtual_data")])
def update_graphs(rows):
    # Rows is the dict values from derived_virtual_data
    # Conditional dff with values from derived virtual data or the actual df
    dff = get_counts_all(data=df) if rows is None\
          else get_counts_all(data=pd.DataFrame(rows))

    return [
        dcc.Graph(
            id='keyword-counts-chart',
            figure={
                "data": [
                    {
                        "x": dff.index,
                        "y": dff.values,
                        "type": "bar",
                    }
                ],
                "layout": {'title': 'keyword counts from full text'},
            },
        )
    ]

@app.callback(
    Output('sentiment-time', "children"),
    [Input('datatable', "derived_virtual_data")])
def update_graphs(rows):

    dff = df.groupby('publish_date').mean() if rows is None\
          else pd.DataFrame(rows).groupby('publish_date').mean()
    dff['sentiment'] = dff['sentiment'].ewm(span = 5).mean() # Smooth

    return [
        dcc.Graph(
            id='sentiment-time-chart',
            figure={
                "data": [
                    {
                        "x": dff.index,
                        "y": dff['sentiment'],
                        "type": "line",
                    }
                ],
                "layout": {'title': 'sentiment vs time'},
            },
        )
    ]

@app.callback(
    Output('sentiment-distribution', "children"),
    [Input('datatable', "derived_virtual_data")])
def update_graphs(rows):

    dff = columns_to_rows(df) if rows is None\
          else columns_to_rows(pd.DataFrame(rows))

    fig = ff.create_distplot([dff[c].dropna() for c in dff.columns],
                             dff.columns, bin_size=.05, show_curve=False)
    fig['layout'].update(title='sentiment distribution')

    return [
        dcc.Graph(
            id='sentiment-dist-chart',
            figure=fig
        )
    ]


if __name__ == '__main__':
    app.run_server(debug=True)
