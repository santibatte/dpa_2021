## MODULE WITH PROJECT GENERAL PARAMETERS





"------------------------------------------------------------------------------"
####################
## Path locations ##
####################


## Ingestion
data_path_csv = "data/raw/Food_Inspections.csv"
ingestion_pickle_loc = "data/pickles/ingest_df.pkl"

## Transformation
transformation_pickle_loc = "outputs/transformation/transformation_df.pkl"

## Feature engineering
fe_pickle_loc_imp_features = "outputs/feature_engineering/fe_df_imp_features.pkl"
fe_pickle_loc_feature_labs = "outputs/feature_engineering/fe_df_feature_labs.pkl"

## Modeling
models_pickle_loc = "outputs/modeling/model_loop.pkl"
X_train_pickle_loc = "outputs/modeling/X_train.pkl"
y_train_pickle_loc = "outputs/modeling/y_train.pkl"
X_test_pickle_loc = "outputs/modeling/X_test.pkl"
y_test_pickle_loc = "outputs/modeling/y_test.pkl"
test_predict_labs_pickle_loc = "outputs/modeling/test_predict_labs.pkl"
test_predict_scores_pickle_loc = "outputs/modeling/test_predict_scores.pkl"

## Model evaluation
metrics_report_pickle_loc = "outputs/model_evaluation/metrics_report.pkl"
aequitas_df_pickle_loc = "outputs/model_evaluation/aequitas_df.pkl"





"------------------------------------------------------------------------------"
########################
## General Parameters ##
########################


## Serious violations parameters

#### Regex pattern to find violation numbers
regex_violations = '\| (.+?). '

#### List of violations considered serious
serious_viols = [str(num) for num in list(range(1, 44 + 1)) + [70]]





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"