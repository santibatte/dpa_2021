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
tests_dir_loc = "src/pipeline/luigi/luigi_tmp_files/"

## AWS and directories
bucket_name = "data-product-architecture-equipo-9"
hist_ingest_path = "ingestion/initial/"
hist_dat_prefix = "historic_inspections_"
cont_ingest_path = "ingestion/consecutive/"
cont_dat_prefix = "consecutive_inspections_"

## Ingestion/extract
local_temp_ingestions = "src/pipeline/luigi/ingestion_tmp/"
year_dir = "YEAR="
month_dir = "MONTH="
data_path_csv = "data/raw/Food_Inspections.csv"
ingestion_pickle_loc = "data/pickles/ingest_df.pkl"
extract_metadata = {}
extract_metadata_index = "ing_time_exec"
extract_metadata_csv_name = "extract_metadata.csv"

## Save_s3
save_s3_metadata = {}
save_s3_metadata_index = "ing_time_exec"
save_s3_metadata_csv_name = "saveS3_metadata.csv"

## Transformation
transformation_pickle_loc = "data/pickles/transformation_df.pkl"
transformation_metadata = {}
transformation_metadata_index = "ing_time_exec"
trans_count = 0
trans_metadata_csv_name = "transformation_metadata.csv"

## Feature engineering
fe_results_pickle_loc = "data/pickles/fe_results.pkl"
fe_metadata = {}
fe_metadata_index = "ing_time_exec"
fe_metadata_csv_name = "feature_engineering_metadata.csv"

## Models training
mt_results_pickle_loc = "data/pickles/mt_results.pkl"
mt_metadata = {}
mt_metadata_index = "ing_time_exec"
mt_metadata_csv_name = "models_training_metadata.csv"

## Model selection
ms_results_pickle_loc = "data/pickles/ms_results.pkl"
ms_metadata = {}
ms_metadata_index = "ing_time_exec"
ms_metadata_csv_name = "model_selection_metadata.csv"

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

