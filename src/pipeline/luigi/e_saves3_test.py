
from luigi.contrib.postgres import CopyToTable ##

import pandas as pd
import luigi
import psycopg2

from src.pipeline.luigi.d_save_s3 import S3Task

from src.utils.utils import (
    get_postgres_credentials
)

csv_local_file = "src/pipeline/luigi/luigi_tmp_files/saveS3_unittest.csv"

class SaveS3UnitTest(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()




    def requires(self):
        return S3Task(ingest_type=self.ingest_type, bucket=self.bucket)

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_unittest.saves3'

    columns = [("Date", "VARCHAR"),
               ("Result", "VARCHAR")]





    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
