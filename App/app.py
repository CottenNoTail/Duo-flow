import psycopg2
import pandas as pd
import pandas.io.sql as psql
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objects as go


# Read data from database
conn = psycopg2.connect(database=os.environ(['database']), user=os.environ(['PG_USER']), password=os.environ(['PG_PSW']), host=password=os.environ(['PG_HOST']), port="5432")
cur = conn.cursor()
df = psql.read_sql("Select * from lightning", conn)

df['dayofyear'] = pd.DatetimeIndex(df['Year-Month']).dayofyear
df['day'] = pd.DatetimeIndex(df['Year-Month']).day
df_month = df.groupby(['year','month'],as_index=False)['number_of_strikes'].agg(['sum']).reset_index()
df_day = df.groupby(['year','day'],as_index=False)['number_of_strikes'].agg(['sum']).reset_index()


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1("Lightning around U.S."),

    html.Div([
        dcc.Graph(
            id='graph-with-slider',

        )
    ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
    html.Div([
        dcc.Dropdown(
            id='month-slider',
            options=[{'label': "Jan", 'value': 1},
                    {'label': "Feb", 'value': 2},
                    {'label': "Mar", 'value': 3},
                    {'label': "Apr", 'value': 4},
                    {'label': "May", 'value': 5},
                    {'label': "Jun", 'value': 6},
                    {'label': "July", 'value': 7},
                    {'label': "Aug", 'value': 8},
                    {'label': "Sep", 'value': 9},
                    {'label': "Oct", 'value': 10},
                    {'label': "Nov", 'value': 11},
                    {'label': "Dec", 'value': 12},

                     ],
            value=7
        ),
        dcc.Graph(id='month-time-series'),
        dcc.Dropdown(
            id='day-slider',
            options=[{'label': 'day ' + str(i), 'value': i} for i in range(1, 32, 1)],
            value=7
        ),
        dcc.Graph(id='day-time-series'),
    ],style={'display': 'inline-block', 'width': '49%'}),

    html.Div(
        dcc.Slider(
            id='year-slider',
            min=df['year'].min(),
            max=df['year'].max(),
            value=2016,
            marks={str(year): str(year) for year in df['year'].unique()},
            step=None
        ),style={'padding': '20px 20px 20px 20px'}),

])

@app.callback(
    Output('graph-with-slider', 'figure'),
    [Input('year-slider', 'value')])
def update_figure(selected_year):

    dff = df[df['year'] == selected_year]
    return {
        'data': [dict(
            x= dff['dayofyear'],
            y = dff['number_of_strikes'],
            mode="markers",
            marker=dict(size = 15, opacity= 0.2, line=dict(width=0.5, color= 'white'))


        )],
        'layout': dict(
            xaxis={'type': 'linear', 'title': 'day of year','range': [0, 360]},
            yaxis={'type': 'linear','title': 'num of strikes', 'range': [0, 500000]},
            margin={'l': 60, 'b': 30, 't': 10, 'r': 0},
            height=450,
            hovermode='closest',

        )
    }


@app.callback(
    Output('month-time-series', 'figure'),
    [Input('month-slider', 'value')])
def update_figure_month(selected_month):
    dff_month = df_month[df_month['month'] == selected_month]
    return {
        'data': [dict(
            x= dff_month['year'],
            y = dff_month['sum'],
            mode="markers+lines",
        )],
        'layout': dict(
            xaxis={'type': 'linear', 'title': 'year','range': [1987, 2020]},
            yaxis={'type': 'linear','title': 'num of strikes', 'range': [0, 30000000]},
            margin={'l': 60, 'b': 30, 't': 10, 'r': 0},
            height=210,
            hovermode='closest',
            showlegend=False,
        )
    }

@app.callback(
    Output('day-time-series', 'figure'),
    [Input('day-slider', 'value')])
def update_figure_day(selected_day):
    dff_day = df_day[df_day['day'] == selected_day]
    return {
        'data': [dict(
            x= dff_day['year'],
            y = dff_day['sum'],
            mode="markers+lines",
        )],
        'layout': dict(
            xaxis={'type': 'linear', 'title': 'year','range': [1987, 2020]},
            yaxis={'type': 'linear','title': 'num of strikes', 'range': [0, 6000000]},
            margin={'l': 60, 'b': 30, 't': 10, 'r': 0},
            height=210,
            hovermode='closest',
            showlegend=False,
        )
    }


if __name__ == '__main__':
    app.run_server(debug=True)
