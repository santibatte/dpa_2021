## MODULE WITH PROJECT ML PARAMETERS





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Python libraries.
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import (
    OneHotEncoder,
    StandardScaler
)
from sklearn.pipeline import Pipeline





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
            max_features=6,
            n_estimators=10,
            max_leaf_nodes=10,
            oob_score=True,
            n_jobs=-1,
            random_state=1111
        ),
        "param_grid": {
            "n_estimators": [100, 300, 500, 800],
            "min_samples_leaf": [3, 5, 7],
            "criterion": ['gini']
        }
    },

    "decision_tree": {
        "model": DecisionTreeClassifier(
            random_state=2222
            ),
        "param_grid": {
            'max_depth': [5, 10, 15, None],
            'min_samples_leaf': [3, 5, 7]
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
