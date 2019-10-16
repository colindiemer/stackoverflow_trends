import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
from read_from_cassandra import get_all_tags_from_cassandra, keys_dict_cassandra, \
    read_one_from_cassandra, datetime_x_y

###############################
# Pulling data from Cassandra #
###############################


all_tags = get_all_tags_from_cassandra()
keys_dict = keys_dict_cassandra(all_tags)



###########
#Plotting #
###########


app = dash.Dash(__name__)
app.scripts.config.serve_locally = True
app.css.config.serve_locally = True
application = app.server

text_style = dict(color='#444', fontFamily='sans-serif', fontWeight=300, fontSize=36)
title = 'Select a Stackoverflow tag and then choose an associated keyword.'

app.layout = html.Div(
    [html.H2(title, style=text_style),
     html.Div([
         dcc.Dropdown(
             id='tag-dropdown',
             options=[{'label': tag, 'value': tag} for tag in all_tags],
             #value=all_tags[0],
             placeholder="Select a Stackoverflow Tag.",
             optionHeight=18
         ),
     ], style={'width': '20%', 'display': 'inline-block', 'fontColor': 'blue'}),
     html.Div([
         dcc.Dropdown(
             id='keyword-dropdown',
             placeholder="Now select an associated keyword.",
             optionHeight=18
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
        dates = read_one_from_cassandra(selected_value_2, selected_value_1)
        return dcc.Graph(figure=go.Figure(data=[go.Bar(x=datetime_x_y(dates)[0],
                                                       y=datetime_x_y(dates)[1],
                                                       marker=go.bar.Marker(color='rgb(26, 118, 255)')
                                                       )], layout={
            'title':'Count of uses of the keyword: {0} within posts tagged with: {1}'.format(selected_value_1, selected_value_2),
        'yaxis': {'title':'Count per month'}
        }
                                          ))
    else:
        pass


if __name__ == '__main__':
    #application.run(host=os.environ["DASH_DNS"], port=80)
    application.run(host='127.0.0.1', port=8050, debug=True)
