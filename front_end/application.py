import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
import redis
import os
from datetime import datetime
from collections import Counter, defaultdict
import random

#######################
# Querying from Redis #
#######################



def get_all_compound_keys(redis):
    """Extracts all (tag, keyword) pairs from redis database."""
    keys = defaultdict(list)
    for key in redis.scan_iter("*"):
        key_pair = tuple(key.decode("utf-8").split(':'))
        if len(key_pair) == 2:  # omit keys corresponding to schema
            keys[key_pair[0]].append(key_pair[1])
    return keys


def read_one_from_redis(tag, keyword):
    """Given a tag and a keyword, extracts the corresponding value (time series). Converts to months"""
    redis_hash = str(tag) + ":" + str(keyword)
    read = r.hgetall(redis_hash)
    dates_extract = list(read.values())[0].decode("utf-8")  # extract bytecode
    dates_only = dates_extract[13:-1]  # Removes WrappedArray(...) bytecode
    dates_split = [s.strip() for s in dates_only.split(',')]
    dates_formatted = ['{:%Y-%m}'.format(datetime.strptime(date, '%Y-%m-%d')) for date in dates_split]
    return dates_formatted


def datetime_x_y(dates_with_repetitions):
    """Give sorted datetimes and associated counts of posts"""
    dates_counter = dict(Counter(dates_with_repetitions))
    dates_sorted = sorted(list(set(dates_with_repetitions)))
    counts = [dates_counter[date] for date in dates_sorted]
    return dates_sorted, counts


def choose_random(keys_dictionary):
    """For debugging purposes, mostly. Chooses a random tag/keyword pair."""
    tag = random.choice(list(keys_dictionary.keys()))
    keyword = random.choice(keys_dictionary[tag])
    return tag, keyword


r = redis.Redis(host=os.environ["REDIS_DNS"], port=6379, db=1)

keys_dict = get_all_compound_keys(r)
all_tags = list(keys_dict.keys())
nestedOptions = keys_dict[all_tags[0]]

############
# Plotting #
############


app = dash.Dash(__name__)
app.scripts.config.serve_locally = True
app.css.config.serve_locally = True
application = app.server

text_style = dict(color='#444', fontFamily='sans-serif', fontWeight=300)
title = 'Select a Stackoverflow tag and then choose an associated keyword.'

app.layout = html.Div(
    [html.H2(title, style=text_style),
     html.Div([
         dcc.Dropdown(
             id='tag-dropdown',
             options=[{'label': tag, 'value': tag} for tag in all_tags],
             value=all_tags[0]
         ),
     ], style={'width': '20%', 'display': 'inline-block'}),
     html.Div([
         dcc.Dropdown(
             id='keyword-dropdown',
         ),
     ], style={'width': '20%', 'display': 'inline-block'}
     ),
     html.Hr(),
     html.Div(id='display-selected-values'),
     ]
)


@app.callback(
    dash.dependencies.Output('keyword-dropdown', 'options'),
    [dash.dependencies.Input('tag-dropdown', 'value')])
def update_date_dropdown(tag):
    return [{'label': i, 'value': i} for i in keys_dict[tag]]


@app.callback(
    dash.dependencies.Output('display-selected-values', 'children'),
    [dash.dependencies.Input('keyword-dropdown', 'value'),
     dash.dependencies.Input('tag-dropdown', 'value')])
def display_graph(selected_value_1, selected_value_2):
    if selected_value_1 and selected_value_2:
        dates = read_one_from_redis(selected_value_2, selected_value_1)
        return dcc.Graph(figure=go.Figure(data=[go.Bar(x=datetime_x_y(dates)[0],
                                                       y=datetime_x_y(dates)[1],
                                                       marker=go.bar.Marker(color='rgb(26, 118, 255)')
                                                       )]
                                          ))
    else:
        pass


if __name__ == '__main__':

    application.run(host=os.environ["DASH_DNS"], port=80)


