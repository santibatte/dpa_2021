cambiar el codigo





from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2

from src.pipeline.luigi.j_feature_engineering import FeatureEngineering

from src.utils.utils import (
    get_postgres_credentials
)


class SaveS3UnitTest(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()





    def requires(self):
        return FeatureEngineering(ingest_type=self.ingest_type, bucket=self.bucket)

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_unittest.feature_engineering'


    columns = [("XXX", "VARCHAR"),
               ("XXX", "VARCHAR"),
               ("XXX", "VARCHAR")]





    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
