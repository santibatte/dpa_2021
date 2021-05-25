import dash
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import plotly.express as px

from dash.dependencies import Output, Input



from src.utils.utils import get_db_conn_sql_alchemy

db_conn_str = get_db_conn_sql_alchemy('../../../conf/local/credentials.yaml')



df= pd.read_sql_table('monitor', con=db_conn_str, schema='dpa_monitor')


#df = pd.read_csv("data/data.csv")


df["score"] = df[['score_label_0','score_label_1']].max(axis=1)
df["prediction_date"] = pd.to_datetime(df["prediction_date"], format="%m/%d/%Y")

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
                    children=[
                        html.Div(
                            children="Pick a Date",
                            className="menu-title"
                            ),
                        # dcc.DatePickerRange(
                        #     id="date-range",
                        #     min_date_allowed=df.prediction_date.min().date(),
                        #     #max_date_allowed=df.prediction_date.max().date(),
                        #     start_date=df.prediction_date.min().date(),
                        #     end_date=df.prediction_date.max().date(),
                        # ),
                        dcc.DatePickerSingle(
                            id="date-single",
                            date=df.prediction_date.min().date(),
                            min_date_allowed=df.prediction_date.min().date(),
                            max_date_allowed=df.prediction_date.max().date(),
                            )
                    ]
                ),
            ],
            className="menu",
        ),
        html.Div(
            children=[
                
                html.Div(
                    children=dcc.Graph(
                        id="score-chart",
                    ),
                    className="card",
                ),
                html.Div(
                    children=dcc.Graph(
                        id="predict-chart",
                    ),
                    className="card",
                ),
            ],
            className="wrapper",
        ),
    ]
)

@app.callback(
    [   
        Output("score-chart", "figure"), 
        Output("predict-chart", "figure")
    ],
    [
        #Input("date-range", "start_date"),
        #Input("date-range", "end_date"),
        Input("date-single", "date")
    ],
)
def update_charts(date):#start_date, end_date):
    # mask = (
    #             (df.prediction_date >= start_date)
    #             & (df.prediction_date <= end_date)
    # )
    
    mask = (df.prediction_date == date)
    filtered_data = df.loc[mask, :]

    scores = px.histogram(filtered_data, 
                        x ="score", 
                        nbins=40, 
                        title="Scores distribution", 
                        marginal="rug",
                        color="model_label",
                        )

    predictios = filtered_data.groupby('model_label').size().reset_index(name="count")
    pred = px.bar(data_frame=predictios, 
                x="model_label", 
                y="count", 
                barmode="group", 
                title="Predictions distribution",
                )
    return scores, pred


if __name__ == "__main__":
    app.run_server(debug=True, port=8051)