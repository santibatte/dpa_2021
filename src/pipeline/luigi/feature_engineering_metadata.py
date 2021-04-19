



from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.feature_engineering import FeatureEngineering

csv_local_file = "src/pipeline/luigi/luigi_tmp_files/feature_engineering_metadata.csv"



class FeatureEngineeringMetadata(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()

    csv_local_file = "src/pipeline/luigi/luigi_tmp_files/feature_engineering_metadata.csv"

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
               ("col_2", "VARCHAR"),
               ("col_3", "VARCHAR"),
               ("col_4", "VARCHAR"),
               ("col_5", "VARCHAR"),
               ("col_6", "VARCHAR"),
               ("col_7", "VARCHAR"),
               ("col_8", "VARCHAR"),
               ("col_9", "VARCHAR")]


    #csv_local_file = "src/pipeline/luigi/luigi_tmp_files/saveS3_metadata.csv"



    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
