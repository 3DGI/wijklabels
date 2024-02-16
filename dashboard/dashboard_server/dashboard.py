from dash import Dash, html, dcc, callback, Output, Input
import geopandas
import plotly.graph_objects as go
import flask

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

server = flask.Flask(__name__) # define flask app.server

app = Dash(__name__, external_stylesheets=external_stylesheets,
           server=server,
           url_base_pathname='/wijklabels/') # call flask server

# run following in command
# gunicorn graph:server -b :8050

COLORS = {"#1a9641": "a++++",
          "#52b151": "a+++",
          "#8acc62": "a++",
          "#b8e17b": "a+",
          "#dcf09e": "a",
          "#ffffc0": "b",
          "#ffdf9a": "c",
          "#febe74": "d",
          "#f69053": "e",
          "#e75437": "f",
          "#d7191c": "g"
          }

neighborhoods = geopandas.read_file("labels_neighborhood_geom.fgb").set_index(["gemeentenaam", "buurtnaam"])
nbhd_long = neighborhoods.drop(columns=["geometry"]).melt(var_name="Energielabel", value_name="Percentage", ignore_index=False).sort_index()
nbhd_long["Percentage"] = nbhd_long["Percentage"] * 100

def plot_buurt(selected_municipality, selected_neighborhood):
    fig = go.Figure(data=[go.Bar(
        x=nbhd_long.loc[(selected_municipality, selected_neighborhood)].Energielabel,
        y=nbhd_long.loc[(selected_municipality, selected_neighborhood)].Percentage,
        marker_color=list(COLORS.keys()),
        hovertemplate = "%{x}: %{y:.1f}%"
    )])
    fig.update_yaxes(range=[0, 60])
    fig.update_layout(title_text=f"{selected_municipality}, {selected_neighborhood}", xaxis_title="Energielabel", yaxis_title="Percentage (%)")
    return fig

@callback(
    Output(component_id='controls-and-graph', component_property='figure'),
    [Input('municipalities-dropdown', 'value'), Input('neighborhoods-dropdown', 'value')]
)
def update_graph(selected_municipality, selected_neighborhood):
    fig = plot_buurt(selected_municipality, selected_neighborhood)
    return fig

@app.callback(
    Output('neighborhoods-dropdown', 'options'),
    [Input('municipalities-dropdown', 'value')])
def set_neighborhoods_options(selected_municipality):
    return neighborhoods.index.get_loc_level(selected_municipality, level=0)[1]

@app.callback(
    Output('neighborhoods-dropdown', 'value'),
    [Input('neighborhoods-dropdown', 'options')])
def set_neighborhoods_value(available_options):
    return available_options[0]

app.layout = html.Div([
    html.Div([
        html.Div(
            children=[
                html.H5(children="Municipality:"),
                dcc.Dropdown(
                    id="municipalities-dropdown",
                    options=neighborhoods.index.get_level_values(0).unique(),
                    value="'s-Gravenhage",
                    clearable=False
                ),
            ], style={'padding': 10, 'flex': 1}
        ),
        html.Div(
            children=[
                html.H5(children="Neighborhood:"),
                dcc.Dropdown(
                    id="neighborhoods-dropdown",
                    value="Bezuidenhout-Midden",
                    clearable=False
                ),
            ], style={'padding': 10, 'flex': 1}
        ),
    ], style={'display': 'flex', 'flexDirection': 'row'}),
    dcc.Graph(figure={}, id="controls-and-graph"),
])

# if __name__ == '__main__':
#     app.run_server(debug=True)