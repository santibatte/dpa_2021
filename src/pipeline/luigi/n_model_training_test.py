

Cambiar el codigo


from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.pipeline.luigi.save_s3 import S3Task

from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.m_model_training import ModelTraining

csv_local_file = "src/pipeline/luigi/luigi_tmp_files/model_training_unittest.csv"

class ModelTrainingTest(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    def requires(self):
        return ModelTraining(ingest_type=self.ingest_type, bucket=self.bucket)

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_unittest.model_training'



    columns = [("XXX", "VARCHAR"),
               ("XXX", "VARCHAR")]





    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
