## MODULE WITH PROJECT GENERAL PARAMETERS





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports

from datetime import (date, datetime)

## Third party imports


## Local application imports





"------------------------------------------------------------------------------"
#########################
## Paths and filenames ##
#########################


## General
metadata_dir_loc = "src/pipeline/luigi/luigi_tmp_files/"

## AWS and directories
bucket_name = "data-product-architecture-equipo-9"
hist_ingest_path = "ingestion/initial/"
hist_dat_prefix = "historic_inspections_"
cont_ingest_path = "ingestion/consecutive/"
cont_dat_prefix = "consecutive_inspections_"

## Ingestion
local_temp_ingestions = "src/pipeline/luigi/ingestion_tmp/"
year_dir = "YEAR="
month_dir = "MONTH="
data_path_csv = "data/raw/Food_Inspections.csv"
ingestion_pickle_loc = "data/pickles/ingest_df.pkl"
ingestion_metadata = {}
ingestion_metadata_index = "ing_time_exec"
ingestion_metadata_csv_name = "extract_metadata.csv"

## Transformation
transformation_pickle_loc = "data/pickles/transformation_df.pkl"
transformation_metadata = {}
transformation_metadata_index = "ing_time_exec"
trans_count = 0
trans_metadata_csv_name = "transformation_metadata.csv"

## Feature engineering
fe_pickle_loc_imp_features = "data/pickles/fe_df_imp_features.pkl"
fe_pickle_loc_feature_labs = "data/pickles/fe_df_feature_labs.pkl"
fe_metadata = {}
fe_metadata_index = "ing_time_exec"
fe_metadata_csv_name = "feature_engineering_metadata.csv"

## Modeling
models_pickle_loc = "data/pickles/model_loop.pkl"
X_train_pickle_loc = "data/pickles/X_train.pkl"
y_train_pickle_loc = "data/pickles/y_train.pkl"
X_test_pickle_loc = "data/pickles/X_test.pkl"
y_test_pickle_loc = "data/pickles/y_test.pkl"
test_predict_labs_pickle_loc = "data/pickles/test_predict_labs.pkl"
test_predict_scores_pickle_loc = "data/pickles/test_predict_scores.pkl"

## Model evaluation
metrics_report_pickle_loc = "data/pickles/metrics_report.pkl"
aequitas_df_pickle_loc = "data/pickles/aequitas_df.pkl"





"------------------------------------------------------------------------------"
########################
## General Parameters ##
########################


## Date when the pipeline is executed
today_info = date.today().strftime('%Y-%m-%d')


## Serious violations parameters

#### Regex pattern to find violation numbers
regex_violations = r'-_(\d+?)\._'

#### List of violations considered serious
serious_viols = [str(num) for num in list(range(1, 44 + 1)) + [70]]


## Parameters for category reduction

#### Dictionary with references to make substitution
cat_reduction_ref = {
    "facility-type": {
        "cat_1": {
            "key_words": [
                "resta",
                "bar",
                "food"
            ],
            "substitution": "restaurant_bar"
        },
        "cat_2": {
            "key_words": [
                "daycare",
                "children",
                "day_care"
            ],
            "substitution": "daycare"
        },
    },
    "city": {
        "cat_1": {
            "key_words": [
                "chica"
            ],
            "substitution": "chicago"
        },
    },
    "inspection-type": {
        "cat_1": {
            "key_words": [
                "re-inspec",
                "reinspec",
            ],
            "substitution": "re_inspec"
        },
        "cat_2": {
            "key_words": [
                "complaint",
            ],
            "substitution": "complaint"
        },
        "cat_3": {
            "key_words": [
                "inspecti",
            ],
            "substitution": "inspection"
        },
        "cat_4": {
            "key_words": [
                "canvas",
            ],
            "substitution": "canvass"
        },
        "cat_5": {
            "key_words": [
                "licen",
            ],
            "substitution": "license"
        },
    },
}


## Classification threshold
classif_theshold = 0.18
reference_at_k = 20/551





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"