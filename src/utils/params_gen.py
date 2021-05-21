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
fe_aws_key = "feature_engineering/"

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
ms_aws_key = "model_selection/"

## Model evaluation - aequitas (bias and fairness) and predictive performance
metrics_report_pickle_loc = "data/pickles/metrics_report.pkl"
aq_results_pickle_loc = "data/pickles/aequitas.pkl"
aq_metadata = {}
aq_metadata_index = "ing_time_exec"
aq_metadata_csv_name = "aequitas_metadata.csv"

## Prediction with new data
pr_results_pickle_loc = "data/pickles/pr_results.pkl"
pr_metadata = {}
pr_metadata_index = "ing_time_exec"
pr_metadata_csv_name = "predict_metadata.csv"





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

#### List with zip codes classification
high_income_zip_codes = [60606, 60601, 60611, 60614, 60603, 60655, 60646, 60605, 60657, 60631, 60661, 60652, 60643, 60610,60634, 60613]
medium_income_zip_codes = [60630, 60656, 60638, 60659, 60641, 60645, 60618, 60607, 60633, 60629, 60639, 60625, 60622, 60628, 60632, 60620, 60617, 60647]
low_income_zip_codes = [60660, 60619, 60651, 60640, 60615, 60626, 60604, 60616, 60623, 60608, 60636, 60649, 60644, 60609, 60612, 60602, 60637, 60624, 60621, 60653, 60654]


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

