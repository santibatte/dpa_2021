#!/usr/bin/env python





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports

## Local application imports

from src.utils.params_gen import (
    data_path_csv,
    ingestion_pickle_loc,
)

from src.etl.ingesta_almacenamiento import (
    guardar_ingesta,
    bucket_name,
    cont_ingest_path,
    hist_ingest_path,
    ingest,
)





"------------------------------------------------------------------------------"
###############
## Functions ##
###############


## Function to test project
def main_execution_function():
    """
    Function to test project
        args:
        returns:
    """

    ##
    ingest(data_path_csv, ingestion_pickle_loc)

    ##
    # guardar_ingesta(bucket_name, cont_ingest_path)





"------------------------------------------------------------------------------"
###############
##
###############

if __name__ == "__main__":
    main_execution_function()
