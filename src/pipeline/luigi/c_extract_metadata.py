

from luigi.contrib.postgres import CopyToTable ##

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.extract import APIDataIngestion

csv_local_file = "src/pipeline/luigi/luigi_tmp_files/extract_metadata.csv"

class ExtractMetadata(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    #bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()

    csv_local_file = "src/pipeline/luigi/luigi_tmp_files/extract_metadata.csv"

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
    columns = [("extraction_time", "VARCHAR"),
               ("raw_cols_deleted", "VARCHAR"),
               ("raw_cols_left", "VARCHAR")]





    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element

