## MODULE WITH TASK TO EXTRACT DATA FROM API





"----------------------------------------------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports

from datetime import (date, datetime)

import pickle

import re

import yaml

import pandas as pd


## Third party imports

import luigi

import joblib


## Local application imports

from src.utils.params_gen import (
    bucket_name,
)

from src.utils.utils import (
    get_s3_resource,
)


from src.etl.ingesta_almacenamiento import (
    ingest,
    request_data_to_API,
    save_local_ingestion,
)





"----------------------------------------------------------------------------------------------------------------------"
################
## Luigi task ##
################


## Task aimed to download data from API
class APIDataIngestion(luigi.Task):


    ## Parameters
    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    ## Run: download data from API depending on the ingestion type
    def run(self):

        ## Obtaining ingestion from API based on the ingestion type
        print("********")
        ingesta = request_data_to_API(self.ingest_type, bucket_name)
        ingesta_df_clean = ingest(pd.DataFrame(ingesta))

        #### Líneas de prueba para ver dimensiones de la ingesta (revisión de unittest)
        print("####################")
        print(ingesta_df_clean.shape)
        print("####################")

        ## Saving ingestion results
        pickle.dump(ingesta_df_clean, open(self.output().path, 'wb'))


    ## Output: storing downloaded information locally
    def output(self):

        local_save_loc = save_local_ingestion(self.ingest_type)

        return luigi.local_target.LocalTarget(local_save_loc)





"----------------------------------------------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"----------------------------------------------------------------------------------------------------------------------"
