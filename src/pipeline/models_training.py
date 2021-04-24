## MODULE TO TRAIN VARIOUS MODELS AND STORE THEM IN A DICTIONARY.





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports


## Third party imports

from sklearn.model_selection import (
    GridSearchCV,
    TimeSeriesSplit
)

from sklearn.model_selection import (
    train_test_split
)


## Local application imports

from src.utils.utils import (
    load_df,
    save_df
)

from src.utils.params_gen import (
    s
)

from src.utils.params_ml import (
    models_dict,
    time_series_splits,
    evaluation_metric,
)





"------------------------------------------------------------------------------"
########################
## Modeling functions ##
########################


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
############################
## Modeling main function ##
############################


## Function desigend to execute all fe functions.
def modeling(fe_results_dict):
    """
    Function desigend to execute all modeling functions.
        args:
            fe_pickle_loc (string): path where the picke obtained from the feature engineering is.
            models_pickle_loc (string): location where the resulting pickle object (best model) will be stored.
        returns:
            -
    """

    ## Implementing magic loop to train various models
    models_mloop, X_train, X_test, y_train, y_test = magic_loop(models_dict, fe_results_dict)

    ## Saving modeling results

    #### Best model
    save_models(sel_model, models_pickle_loc)

    #### Data used to train the model
    save_models(X_train, X_train_pickle_loc)
    save_models(y_train, y_train_pickle_loc)
    save_models(X_test, X_test_pickle_loc)
    save_models(y_test, y_test_pickle_loc)

    #### Results from testing the model
    save_models(test_predict_labs, test_predict_labs_pickle_loc)
    save_models(test_predict_scores, test_predict_scores_pickle_loc)

    print("\n** Modeling module successfully executed **\n")





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
