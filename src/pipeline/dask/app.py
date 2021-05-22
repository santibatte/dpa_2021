import dash
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import plotly.express as px

from dash.dependencies import Output, Input

from src.utils.utils import get_db_conn_sql_alchemy

db_conn_str = get_db_conn_sql_alchemy('../../../conf/local/credentials.yaml')



df= pd.read_sql_table(monitor, con=db_conn_str, schema=dpa_monitor)


#df = pd.read_csv("data/data.csv")
df["score"] = df[['score_label_0','score_label_1']].max(axis=1)

scores = px.histogram(df,
                        x ="score",
                        nbins=40,
                        title="Scores distribution",
                        marginal="rug",
                        color="model_label",
                        )

predictions = df.groupby('model_label').size().reset_index(name="count")
pred = px.bar(data_frame=predictions,
                x="model_label",
                y="count",
                barmode="group",
                title="Predictions distribution",
                )

external_stylesheets = [
    {
        "href": "https://fonts.googleapis.com/css2?"
        "family=Lato:wght@400;700&display=swap",
        "rel": "stylesheet",
    },
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Chicago Foods Inspections"

app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.P(children="🌭", className="header-emoji"),
                html.H1(
                    children="Chicago Foods Inspections", className="header-title"
                ),
                html.P(
                    children="Equipo 9 Model Monitoring",
                    className="header-description",
                ),
            ],
            className="header",
        ),
        html.Div(
            children=[

                html.Div(
                    children=dcc.Graph(
                        id="score-chart",
                        figure=scores
                    ),
                    className="card",
                ),
                html.Div(
                    children=dcc.Graph(
                        id="predict-chart",
                        figure=pred
                    ),
                    className="card",
                ),
            ],
            className="wrapper",
        ),
    ]
)


if __name__ == "__main__":
    app.run_server(debug=True, port=8051)
