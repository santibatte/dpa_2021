



from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.feature_engineering import FeatureEngineering




class SaveS3Metadata(CopyToTable):

    def requires(self):
        return FeatureEngineering(ingest_type=self.ingest_type, bucket=self.bucket)

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_metadata.feature_engineering'


## ADAPTAR al numero de columnas correctas
    columns = [("col_1", "VARCHAR"),
               ("col_2", "VARCHAR")]


    csv_local_file = "src/pipeline/luigi/luigi_tmp_files/saveS3_metadata.csv"



    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
