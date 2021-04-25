## MODULE TO TRAIN VARIOUS MODELS AND STORE THEM IN A DICTIONARY.





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports

import pickle

from datetime import (date, datetime)


## Third party imports

from sklearn.model_selection import (
    GridSearchCV,
    TimeSeriesSplit
)

from sklearn.model_selection import (
    train_test_split
)

import pandas as pd


## Local application imports

from src.utils.params_ml import (
    models_dict,
    time_series_splits,
    evaluation_metric,
)

from src.utils.params_gen import (
    metadata_dir_loc,
    mt_metadata,
    mt_metadata_index,
    mt_metadata_csv_name
)





"------------------------------------------------------------------------------"
###############################
## Models training functions ##
###############################


## Run magic loop to train a selection of models with various parameters.
def magic_loop(models_dict, fe_results_dict):
    """
    Run magic loop to train a selection of models with various parameters.

    :param models_dict: (dict) - models and parameters that will be trained
    :param fe_results_dict: (dict) - dictionary with all feature engineering results

    :return:
    """


    ## Splitting data in train and test
    X_train, X_test, y_train, y_test = train_test_split(
        fe_results_dict["df_imp_engineered_features"],
        fe_results_dict["data_labels"],
        test_size=0.3
    )


    ## Training models selected in magic loop

    #### Dictionary where all the results will be stored
    models_mloop = {}

    #### Magic loop
    for mdl in models_dict:

        model = models_dict[mdl]["model"]

        grid_search = GridSearchCV(model,
                               models_dict[mdl]["param_grid"],
                               cv=TimeSeriesSplit(n_splits=time_series_splits),
                               scoring=evaluation_metric,
                               return_train_score=True,
                               n_jobs=-1
                               )
        grid_search.fit(X_train, y_train)

        models_mloop[mdl] = {
            "best_estimator": grid_search.best_estimator_,
            "best_estimator_score": grid_search.best_score_
        }


    return models_mloop, X_train, X_test, y_train, y_test





"------------------------------------------------------------------------------"
###################################
## Models training main function ##
###################################


## Function desigend to execute all fe functions.
def models_training(fe_results_dict, mt_results_pickle_loc):
    """
    Function desigend to execute all modeling functions.
        args:
            fe_pickle_loc (string): path where the picke obtained from the feature engineering is.
            models_pickle_loc (string): location where the resulting pickle object (best model) will be stored.
        returns:
            -
    """

    ## Storing time execution metadata
    mt_metadata[mt_metadata_index] = str(datetime.now())

    ## Implementing magic loop to train various models
    models_mloop, X_train, X_test, y_train, y_test = magic_loop(models_dict, fe_results_dict)


    ## Saving models training results

    #### Dictionary with all module results
    mt_results_dict = {
        "trained_models": models_mloop,
        "training_data": X_train,
        "training_labels": y_train,
        "test_data": X_test,
        "test_labels": y_test,
    }

    #### Saving dictionary with results as pickle
    pickle.dump(mt_results_dict, open(mt_results_pickle_loc, "wb"))

    print("\n** Models training module successfully executed **\n")


    ## Saving relevant module metadata

    #### Number of models trained
    mt_metadata["no_models_trained"] = len(models_mloop)

    #### Types of models trained
    mt_metadata["type_models_trained"] = " | ".join([mdl for mdl in models_dict])

    #### Converting metadata into dataframe and saving locally
    df_meta = pd.DataFrame.from_dict(mt_metadata, orient="index").T
    df_meta.set_index(mt_metadata_index, inplace=True)
    write_csv_from_df(df_meta, metadata_dir_loc, mt_metadata_csv_name)


    return mt_results_dict





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
