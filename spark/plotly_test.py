import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import redis
import os
from datetime import datetime
from collections import Counter


def read_one_from_redis(table, keyword_index):
    redis_hash = table + ":" + keyword_index
    read = r.hgetall(redis_hash)
    dates_extract = list(read.values())[0].decode("utf-8")  # extract bytecode
    dates_only = dates_extract[13:-1]  # Removes WrappedArray(...) bytecode
    dates_split = [s.strip() for s in dates_only.split(',')]
    dates_formatted = [datetime.strptime(date, '%Y-%m-%d').date() for date in dates_split]
    return dates_formatted


def datetime_x_y(dates_with_repetitions):
    dates_counter = dict(Counter(dates_with_repetitions))
    dates = sorted(list(set(dates_with_repetitions)))
    counts = [dates_counter[date] for date in dates]
    return dates, counts


r = redis.Redis(host=os.environ["POSTGRES_DNS"], port=6379, db=0)
test = read_one_from_redis('mo_test_1', '349')

app = dash.Dash('Dash Hello World')

text_style = dict(color='#444', fontFamily='sans-serif', fontWeight=300)
# plotly_figure = dict(data=[dict(x=[1, 2, 3], y=[2, 4, 8])])
# plotly_figure = dict(data=[dict(x=datetime_x_y(test)[0], y=datetime_x_y(test)[1])])

app.layout = html.Div([
    html.H2('Tag Usage Over Time', style=text_style),
    dcc.Graph(figure=go.Figure(
        data=[go.Bar(
            x=datetime_x_y(test)[0],
            y=datetime_x_y(test)[1],
            name='Title',
            marker=go.bar.Marker(
                color='rgb(26, 118, 255)'))]))])

# app.layout = html.Div([

#     html.H2('My First Dash App', style=text_style),
#     html.P('Enter a Plotly trace type into the text box, such as histogram, bar, or scatter.', style=text_style),
#     dcc.Input(id='text1', placeholder='box', value=''),
#     dcc.Graph(id='plot1', figure=plotly_figure),
# ])
#
#
# @app.callback(Output('plot1', 'figure'), [Input('text1', 'value')])
# def text_callback(text_input):
#     return {'data': [dict(x=[1, 2, 3], y=[2, 4, 8], type=text_input)]}
#
#
app.server.run()
