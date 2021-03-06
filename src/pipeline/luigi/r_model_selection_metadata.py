from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.q_model_selection_test import ModelSelectionUnitTest

csv_local_file = "src/pipeline/luigi/luigi_tmp_files/model_selection_metadata.csv"

class ModelSelectionMetadata(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    def requires(self):
        return ModelSelectionUnitTest(ingest_type=self.ingest_type, bucket=self.bucket)

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_metadata.model_selection'


    ## Metadata columns saved in RDS file
    columns = [("execution_time", "VARCHAR"),
               ("training_score", "VARCHAR"),
               ("selected_model", "VARCHAR")]



    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element

