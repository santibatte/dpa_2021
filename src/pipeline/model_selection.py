## MODULE TO SELECT BEST TRAINED MODEL





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

    fe_pickle_loc_imp_features,
    fe_pickle_loc_feature_labs,

    models_pickle_loc,
    X_train_pickle_loc,
    y_train_pickle_loc,
    X_test_pickle_loc,
    y_test_pickle_loc,
    test_predict_labs_pickle_loc,
    test_predict_scores_pickle_loc,
)

from src.utils.params_ml import (
    models_dict,
    time_series_splits,
    evaluation_metric,
)





"------------------------------------------------------------------------------"
#################################
## Generic ancillary functions ##
#################################


## Loading transformation pickle as dataframe for transformation pipeline.
def load_features(path):
    """
    Loading fe pickle as dataframe from fe pipeline.
        args:
            path (string): location where the pickle that will be loaded is.
        returns:
            -
    """

    df = load_df(path)

    return df



## Save best model form magic loop as pickle.
def save_models(selected_model, path):
    """
    Save best model form magic loop as pickle.
        args:
            selected_model (dataframe): model that got best performance in magic loop.
            path (string): location where the pickle object will be stored.
        returns:
            -
    """

    save_df(selected_model, path)





"------------------------------------------------------------------------------"
########################
## Modeling functions ##
########################


## Run magic loop to train a selection of models with various parameters.
def magic_loop(models_dict, df_imp_features_prc, df_labels):
    """
    Run magic loop to train a selection of models with various parameters.

    :param models_dict: (dict) - models and parameters that will be trained
    :param df_imp_features_prc: (dataframe) - engineered data features
    :param df_labels: (dataframe) - data training labels

    :return:
    """


    ## Splitting data in train and test
    X_train, X_test, y_train, y_test = train_test_split(df_imp_features_prc, df_labels, test_size=0.3)


    ##
    models_mloop = {}
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

    # sel_model = models_mloop[select_best_model(models_mloop)]["best_estimator"]
    #
    #
    # return sel_model, X_train, X_test, y_train, y_test



## Selecting best trained model based on estimator score
def select_best_model(mt_results_dict):
    """
    """

    best_model = "_no_best_model"
    bench = 0

    for mdl in mt_results_dict["models_mloop"]:
        if models_mloop[mdl]["best_estimator_score"] > bench:
            best_model = mdl
            bench = models_mloop[mdl]["best_estimator_score"]

    print("\n++The model with the best performance is: {} (score: {})".format(best_model, round(bench, 6)))

    return best_model



## Testing model with test data set.
def best_model_predict_test(sel_model, X_test):
    """
    Testing model with test data set.
        args:
            sel_model (sklearn model): best model obtained from magic loop.
            X_test (numpy array): dataset to test best model.
        returns:
            test_predict_labs (array): labels predicted by best model.
            test_predict_scores (array): probabilities related with classification by best model.
    """

    ## Predict test labels and probabilities with selected model.
    test_predict_labs = sel_model.predict(X_test)
    test_predict_scores = sel_model.predict_proba(X_test)

    return test_predict_labs, test_predict_scores





"------------------------------------------------------------------------------"
###################################
## Model selection main function ##
###################################


## Function desigend to execute all ms functions.
def model_selection(mt_results_dict, ms_results_pickle_loc):
    """
    Function desigend to execute all modeling functions.
        args:
            fe_pickle_loc (string): path where the picke obtained from the feature engineering is.
            models_pickle_loc (string): location where the resulting pickle object (best model) will be stored.
        returns:
            -
    """

    ## Selecting best trained model from magic_loop
    best_model = select_best_model(mt_results_dict)

    ## Testing best model with test data
    test_predict_labs, test_predict_scores = best_model_predict_test(best_model, mt_results_dict["training_data"])

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


    return ms_results_dict





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
