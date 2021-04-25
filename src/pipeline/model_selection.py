## MODULE TO SELECT BEST TRAINED MODEL





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports

import pickle


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

from src.utils.params_ml import (
    models_dict,
    time_series_splits,
    evaluation_metric,
)





"------------------------------------------------------------------------------"
########################
## Modeling functions ##
########################


## Selecting best trained model based on estimator score
def select_best_model(mt_results_dict):
    """
    Selecting best trained model based on estimator score

    :param mt_results_dict: (dict) - dictionary with all the results form the `model_training` module

    :return best_model: (sklearn model) - model with the best estimator score
    """

    model_bench = "_no_result"
    bench = 0

    for mdl in mt_results_dict["models_mloop"]:
        if models_mloop[mdl]["best_estimator_score"] > bench:
            model_bench = mdl
            bench = models_mloop[mdl]["best_estimator_score"]

    print("\n++The model with the best performance is: {} (score: {})".format(model_bench, round(bench, 6)))

    best_model = mt_results_dict["models_mloop"][model_bench]["best_estimator"]

    return best_model



## Testing model with test data set.
def best_model_predict_test(best_model, X_test):
    """
    Testing model with test data set.
        args:
            best_model (sklearn model): best model obtained from magic loop.
            X_test (numpy array): dataset to test best model.
        returns:
            test_predict_labs (array): labels predicted by best model.
            test_predict_scores (array): probabilities related with classification by best model.
    """

    ## Predict test labels and probabilities with selected model.
    test_predict_labs = best_model.predict(X_test)
    test_predict_scores = best_model.predict_proba(X_test)

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
        returns:
            -
    """

    ## Selecting best trained model from magic_loop
    best_model = select_best_model(mt_results_dict)

    ## Testing best model with test data
    test_predict_labs, test_predict_scores = best_model_predict_test(best_model, mt_results_dict["training_data"])


    ## Saving modeling results

    #### Dictionary with all module results
    ms_results_dict = {
        "best_trained_model": best_model,
        "model_test_predict_labels": test_predict_labs,
        "model_test_predict_scores": test_predict_scores
    }

    #### Saving dictionary with results as pickle
    pickle.load(ms_results_dict, open(ms_results_pickle_loc, "wb"))

    print("\n** Model selection module successfully executed **\n")


    return ms_results_dict





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
