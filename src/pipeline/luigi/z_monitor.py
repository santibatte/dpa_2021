from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.utils.params_gen import api_monitor_data

from src.pipeline.luigi.y_store_predictions_api import StorePredictionsApi

csv_local_file = api_monitor_data





class Monitor(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    def requires(self):
        return StorePredictionsApi(ingest_type=self.ingest_type, bucket=self.bucket)

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_monitor.monitor'


    ## Metadata columns saved in RDS file
    columns = [
        ("id_client", "VARCHAR"),
        ("prediction_date", "VARCHAR"),
        ("model_label", "VARCHAR"),
        ("score_label_0", "VARCHAR"),
        ("score_label_1", "VARCHAR"),
    ]



    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
