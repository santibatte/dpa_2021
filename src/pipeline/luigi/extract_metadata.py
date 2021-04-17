

from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.extract import APIDataIngestion

class ExtractMetadata(CopyToTable):

    def requires(self):

        return APIDataIngestion(self.ingest_type)

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_metadata.extract'


## ADAPTAR al numero de columnas correctas
    columns = [("col_1", "VARCHAR"),
               ("col_2", "VARCHAR")]


    csv_local_file = "src/pipeline/luigi/luigi_tmp_files/extract_metadata.csv"



    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
