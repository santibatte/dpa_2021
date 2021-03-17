## MODULE TO EXTRACT DATA FROM API





"----------------------------------------------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports

from datetime import (date, datetime)

import pickle

import re

import yaml


## Third party imports

import luigi

import joblib


## Local application imports

from src.utils.params_gen import (
    bucket_name,
)

from src.etl.ingesta_almacenamiento import (
    guardar_ingesta,
    save_local_ingestion,
)





"----------------------------------------------------------------------------------------------------------------------"
################
## Luigi task ##
################


## Task aimed to download data from API
class APIDataIngestion(luigi.Task):


    ## Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    ## Run: download data from API depending on the ingestion type
    def run(self):

        ## Obtaining ingestion from API based on the ingestion type
        ingesta = guardar_ingesta(self.ingest_type, bucket_name)

        ## Saving ingestion results
        pickle.dump(ingesta, open(self.output().path, 'wb'))


    ## Output: storing downloaded information locally
    def output(self):

        local_save_loc = save_local_ingestion(self.ingest_type)

        return luigi.local_target.LocalTarget(local_save_loc)





"----------------------------------------------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"----------------------------------------------------------------------------------------------------------------------"