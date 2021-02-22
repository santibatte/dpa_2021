#!/usr/bin/env python





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports

## Local application imports

from src.pipeline.ingesta_almacenamiento import (
    guardar_ingesta,
    bucket_name,
    cont_ingest_path,
    hist_ingest_path
)





"------------------------------------------------------------------------------"
###############
## Functions ##
###############


## Function to test project
def test_function():
    """
    Function to test project
        args:
        returns:
    """

    ##
    guardar_ingesta(bucket_name, cont_ingest_path)





"------------------------------------------------------------------------------"
###############
##
###############

if __name__ == "__main__":
    test_function()
