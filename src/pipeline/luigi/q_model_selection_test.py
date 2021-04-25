


Cambiar el codigo


from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.pipeline.luigi.q_model_selection import ModelSelection

from src.utils.utils import (
    get_postgres_credentials
)


csv_local_file = "src/pipeline/luigi/luigi_tmp_files/model_selection_unittest.csv"

class ModelSelectionUnitTest(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    def requires(self):
        return ModelSelection(ingest_type=self.ingest_type, bucket=self.bucket)

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_unittest.model_selection'

    columns = [("XXX", "VARCHAR"),
               ("XXX", "VARCHAR"),
               ("XXX", "VARCHAR")]

    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
