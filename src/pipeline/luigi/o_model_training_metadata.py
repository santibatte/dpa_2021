from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.n_model_training_test import ModelTrainingTest

csv_local_file = "src/pipeline/luigi/luigi_tmp_files/model_training_metadata.csv"



class ModelTrainingMetadata(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()

    csv_local_file = "src/pipeline/luigi/luigi_tmp_files/feature_engineering_metadata.csv"

    def requires(self):
        return ModelTrainingTest(ingest_type=self.ingest_type, bucket=self.bucket)

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_metadata.model_training'


## ADAPTAR al numero de columnas correctas
    columns = [("execution_time", "VARCHAR"),
               ("shape_after_fe", "VARCHAR")]


    #csv_local_file = "src/pipeline/luigi/luigi_tmp_files/saveS3_metadata.csv"



    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
