## MODULE WITH PROJECT ML PARAMETERS





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports


## Third party imports
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import (
    OneHotEncoder,
    StandardScaler
)
from sklearn.pipeline import Pipeline


## Local application imports





"------------------------------------------------------------------------------"
###################
## ML parameters ##
###################


## Pipelines for processing data.
categoric_pipeline = Pipeline([
    ('hotencode',OneHotEncoder())
])
numeric_pipeline = Pipeline([
    ('std_scaler', StandardScaler())
])


## Models and parameters
models_dict = {

    "random_forest": {
        "model": RandomForestClassifier(
            max_features=10,
            n_estimators=10,
            max_leaf_nodes=50,
            oob_score=True,
            n_jobs=-1,
            random_state=1111
        ),
        "param_grid": {
            "n_estimators": [5, 7],
            "min_samples_leaf": [10],
            "criterion": ['gini']
        }
    },

    "decision_tree": {
        "model": DecisionTreeClassifier(
            random_state=2222
            ),
        "param_grid": {
            'max_depth': [10, 15],
            'min_samples_leaf': [5]
        }
    },

}


## Additional parameters for cv_grid
time_series_splits = 8
evaluation_metric = "accuracy"
feature_importance_theshold = 0.001
tag_non_relevant_cats = "other_nr_categories"





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
