#!/usr/bin/env python





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports


## Third party imports


## Local application imports

from src.utils.params_gen import (
    data_path_csv,
    ingestion_pickle_loc,
    transformation_pickle_loc,
    fe_pickle_loc_imp_features,
    fe_pickle_loc_feature_labs,
    fe_pickle_loc_imp_features,
    fe_pickle_loc_feature_labs,
    y_test_pickle_loc,
    test_predict_scores_pickle_loc,
)

from src.etl.ingesta_almacenamiento import (
    bucket_name,
    cont_ingest_path,
    hist_ingest_path,

    guardar_ingesta,
    ingest,
)

from src.etl.transformation import transform

from src.pipeline.feature_engineering import feature_engineering

from src.pipeline.modeling import modeling

from src.pipeline.model_evaluation import model_evaluation





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

    ##
    ingest(data_path_csv, ingestion_pickle_loc)

    ##
    # guardar_ingesta(bucket_name, cont_ingest_path)

    ##
    transform(ingestion_pickle_loc, transformation_pickle_loc)

    ##
    feature_engineering(transformation_pickle_loc, fe_pickle_loc_imp_features, fe_pickle_loc_feature_labs)

    ##
    # modeling(fe_pickle_loc_imp_features, fe_pickle_loc_feature_labs)

    ##
    # model_evaluation(y_test_pickle_loc, test_predict_scores_pickle_loc)





"------------------------------------------------------------------------------"
################################
## Execution of main function ##
################################

if __name__ == "__main__":
    main_execution_function()
