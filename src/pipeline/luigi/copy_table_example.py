
## leer de un .csv en local y guardar en postgres


## Ver de agregar las credentials.yaml

from luigi.contrib.postgres import CopyToTable

import pandas as pd
import luigi
#import psycopg2
import yaml

from src.utils.utils import (
    get_s3_resource,
    get_s3_resource_luigi,
    get_key,
    get_postgres_credentials
)


class CopyTableExample(CopyToTable):
    
    credentials = get_postgres_credentials("conf/local/credentials.yaml")

    user = credentials['user']
    password = credentials['pass']
    database = credentials['db']
    host = credentials['host']
    port = credentials['port']
    table = 'metadata.example'

    columns = [("col_1", "VARCHAR"),
               ("col_2", "VARCHAR")]


    csv_local_file = "src/pipeline/luigi/luigi_tmp_files/example_df.csv"
    #path_full = local_temp_ingestions + self.ingest_type + "/" + self.path_date + path_file


    def rows(self):
        reader = pd.read_csv(csv_local_file, header=None)

        for element in reader.itertuples(index=False):
            yield element
