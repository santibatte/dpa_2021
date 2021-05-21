## MODULE TO MAKE PREDICTIONS OF NEW DATA





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports

import sys

from datetime import (date, datetime)

import pickle


## Third party imports
import pandas as pd
pd.set_option("display.max_columns", 10)

import unittest

import marbles.core

from io import StringIO


## Local application imports

from src.utils.data_dict import (
    data_dict,
    data_created_dict
)

from src.utils.utils import (
    json_dump_dict,
    write_csv_from_df
)

from src.utils.params_gen import (
    metadata_dir_loc,
    tests_dir_loc,
    pr_metadata,
    pr_metadata_index,
    pr_metadata_csv_name,
)





"------------------------------------------------------------------------------"
#######################################
## Feature engineering main function ##
#######################################


## Function desigend to execute all fe functions.
def predict(sel_model, fe_results, pr_results_pickle_loc):
    """
    Function desigend to execute all fe functions.
        args:
        returns:
            -
    """

    ## Storing time execution metadata
    pr_metadata[pr_metadata_index] = str(datetime.now())

    #### Data IDs
    data_ids = fe_results["data_labels"].index
    data_features = fe_results["df_imp_engineered_features"]

    ## Predicting for every entry and attaching the ids info
    dfp = pd.DataFrame.from_dict(
        {
            "ids": data_ids,
            "prediction_date": [str(datetime.now())[:10]]*len(data_ids),
            "model_label": sel_model.predict(data_features),
            "score_label_0": sel_model.predict_proba(data_features)[:, 0],
            "score_label_1": sel_model.predict_proba(data_features)[:, 1],
        }
    )
    dfp.set_index("ids", inplace=True)

    ## Saving results as pickle and storing them in s3
    pickle.dump(dfp, open(pr_results_pickle_loc, "wb"))

    print("\n** Prediction module successfully executed **\n")


    ## Working with module's metadata

    #### Model used to make the predicions
    pr_metadata["predict_model"] = str(sel_model)

    #### Metadata: percentage of positives (1's)
    pr_metadata["percentage_positives"] = round(dfp["model_label"].value_counts(normalize=True)[1], 2)

    #### Average score for positives (1's)
    pr_metadata["mean_score_positives"] = round(dfp["score_label_1"].mean(), 2)

    #### Converting metadata into dataframe and saving locally
    df_meta = pd.DataFrame.from_dict(pr_metadata, orient="index").T
    df_meta.set_index(pr_metadata_index, inplace=True)
    write_csv_from_df(df_meta, metadata_dir_loc, pr_metadata_csv_name)


    return dfp





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"

