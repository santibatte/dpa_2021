
from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.h_transform_test import TransformationUnitTest

csv_local_file = "src/pipeline/luigi/luigi_tmp_files/transformation_metadata.csv"

class TransformationMetadata(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()

    csv_local_file = "src/pipeline/luigi/luigi_tmp_files/transformation_metadata.csv"

    def requires(self):
        return TransformationUnitTest(ingest_type=self.ingest_type, bucket=self.bucket) ##_

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_metadata.transformation'


## ADAPTAR al numero de columnas correctas
    columns = [("execution_date", "VARCHAR"),
               ("number_of_transformations", "VARCHAR"),
               ("new_columns", "VARCHAR")]


    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element

