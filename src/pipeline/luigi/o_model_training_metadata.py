
cambiar este codigo de training metadata




from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.feature_engineering import FeatureEngineering

modificar linea de abajo
csv_local_file = "src/pipeline/luigi/luigi_tmp_files/model_training_metadata.csv"



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
    table = 'dpa_unittest.model_training'


## ADAPTAR al numero de columnas correctas
    columns = [("execution_time", "VARCHAR"),
               ("shape_prior_fe", "VARCHAR"),
               ("num_features", "VARCHAR"),
               ("name_features", "VARCHAR"),
               ("num_cat_features", "VARCHAR"),
               ("name_cat_features", "VARCHAR"),
               ("num_num_features", "VARCHAR"),
               ("name_num_features", "VARCHAR"),
               ("shape_after_fe", "VARCHAR")]


    #csv_local_file = "src/pipeline/luigi/luigi_tmp_files/saveS3_metadata.csv"



    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
