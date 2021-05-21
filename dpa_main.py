#!/usr/bin/env python





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports

import pickle


## Third party imports

import pandas as pd


## Local application imports

from src.utils.params_gen import (
    data_path_csv,
    transformation_pickle_loc,
    fe_results_pickle_loc,
    mt_results_pickle_loc,
    ms_results_pickle_loc,
    pr_results_pickle_loc,
)

from src.etl.ingesta_almacenamiento import (
    bucket_name,
    cont_ingest_path,
    hist_ingest_path,
    ingest,
)

from src.etl.transformation import transform

from src.pipeline.feature_engineering import feature_engineering

from src.pipeline.models_training import models_training

from src.pipeline.model_selection import model_selection

from src.pipeline.predict import predict





"------------------------------------------------------------------------------"
#############################################
## Function to coordinate pipeline modules ##
#############################################


## Function to test project
def main_execution_function():
    """
    Function to test project
        args:
        returns:
    """

    ## Creating dataframe from local data
    df = pd.read_csv(data_path_csv)

    ##
    df = ingest(df)

    ##
    # guardar_ingesta(bucket_name, cont_ingest_path)

    ##
    df = transform(df, transformation_pickle_loc)

    ##
    fe_results_dict = feature_engineering(df, fe_results_pickle_loc)

    ##
    mt_results_dict = models_training(fe_results_dict, mt_results_pickle_loc)

    ##
    ms_results_dict = model_selection(mt_results_dict, ms_results_pickle_loc)

    ##
    dfp = predict(ms_results_dict["best_trained_model"], fe_results_dict, pr_results_pickle_loc)





"------------------------------------------------------------------------------"
################################
## Execution of main function ##
################################

if __name__ == "__main__":
    main_execution_function()