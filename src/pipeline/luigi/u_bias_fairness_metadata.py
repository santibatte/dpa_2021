from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
import psycopg2


from src.utils.utils import (
    get_postgres_credentials
)

from src.pipeline.luigi.t_bias_fairness_test import BiasFairnessUnitTest

csv_local_file = "src/pipeline/luigi/luigi_tmp_files/aequitas_metadata.csv"

class BiasFairnessMetadata(CopyToTable):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    def requires(self):
        return BiasFairnessUnitTest(ingest_type=self.ingest_type, bucket=self.bucket)

    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa_metadata.bias_fairness'


    ## Metadata columns saved in RDS file
    columns = [("execution_time", "VARCHAR"),
               ("value_k", "VARCHAR"),
               ("v_group", "VARCHAR"),
               ("FOR_p", "VARCHAR"),
               ("FNR_p", "VARCHAR")]



    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
