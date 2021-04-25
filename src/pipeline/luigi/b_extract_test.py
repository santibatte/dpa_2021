
from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.pipeline.luigi.a_extract import APIDataIngestion

from src.utils.utils import (
    get_postgres_credentials
)


csv_local_file = "src/pipeline/luigi/luigi_tmp_files/extract_unittest.csv"

class ExtractUnitTest(CopyToTable):

    ingest_type = luigi.Parameter()


    def requires(self):

        return APIDataIngestion(self.ingest_type)

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_unittest.extract'

    columns = [("XXX1", "VARCHAR"),
               ("XXX2", "VARCHAR")]

    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
