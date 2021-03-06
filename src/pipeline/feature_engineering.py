## MODULE TO PERFORM FEATURE ENGINEERING ON DATA





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Python libraries

import sys

from sklearn.model_selection import (
    GridSearchCV,
    TimeSeriesSplit
)
from sklearn.model_selection import (
    train_test_split,
    cross_val_score
)
from sklearn.compose import ColumnTransformer


## Ancillary modules

from src.utils.data_dict import (
    data_dict
)

from src.utils.utils import (
    json_dump_dict,
    load_df,
    save_df
)

from src.utils.params import (
    categoric_pipeline,
    numeric_pipeline,
    models_dict,
    time_series_splits,
    evaluation_metric,
    feature_importance_theshold,
    tag_non_relevant_cats,
    transformation_pickle_loc,
    fe_pickle_loc_imp_features,
    fe_pickle_loc_feature_labs,
)

from src.pipelines.transformation import (
    data_created_dict
)

import pandas as pd
pd.set_option("display.max_columns", 10)





"------------------------------------------------------------------------------"
#################################
## Generic ancillary functions ##
#################################


## Loading transformation pickle as dataframe for transformation pipeline.
def load_transformation(path):
    """
    Loading transformation pickle as dataframe for transformation pipeline.
        args:
            path (string): location where the pickle that will be loaded is.
        returns:
            df (dataframe): dataframe with features obtained from the transformation module.
    """

    df = load_df(path)

    return df



## Save fe data frame as pickle.
def save_fe(df, path):
    """
    Save fe data frame as pickle.
        args:
            df (dataframe): fe resulting dataframe.
            path (string): location where the pickle object will be stored.
        returns:
            -
    """

    save_df(df, path)



## Identifying the original features related to the processed features
def find_mother(row, ohe_dict):
    """
    Identifying the original features related to the processed features
        args:
            row (dataframe row): processed feature that will be traced to its mother.
            ohe_dict (dict): dictionary that stores dummy columns derived from the one hot encoding and the original feature.
        returns:
            mother (string): mother feature of the processed feature.
    """

    for key in ohe_dict:
        if row in ohe_dict[key]:
            mother = key
            return mother

    mother = row

    return mother



## Synthesis of feature importance analysis.
def feature_cleaning_dict(feature_importance, ohe_dict):
    """
    Synthesis of feature importance analysis.
        args:
            feature_importance (dataframe): table with complete result of feature importance analysis.
            ohe_dict (dict): dictionary that stores dummy columns derived from the one hot encoding and the original feature.
        returns:
            fe_cln_dict (dictionary): dict with synthesis of feature importance analysis.
    """

    m1 = feature_importance["Important"] == True
    important_features = list(feature_importance.loc[m1, "Mother_feature"].unique())

    fe_cln_dict = {}

    for imp_f in important_features:

        m2 = feature_importance["Mother_feature"] == imp_f

        if imp_f in ohe_dict:
            fe_cln_dict[imp_f] = {
                "data_type": "categoric",
                "important_categories": list(feature_importance.loc[(m1 & m2), "Feature"])
            }
        else:
            if imp_f in data_dict:
                fe_cln_dict[imp_f] = {
                    "data_type": data_dict[imp_f]["data_type"]
                }
            elif imp_f in data_created_dict:
                fe_cln_dict[imp_f] = {"data_type": data_created_dict[imp_f]["data_type"]}
            else:
                raise NameError("Rob_error: the important feature is not in any data dictionary")


    return fe_cln_dict



## Cleaning the feature dataframe based on the feature importance analysis.
def apply_feature_selection(fe_cln_dict, df):
    """
    Cleaning the feature dataframe based on the feature importance analysis.
        args:
             dict with synthesis of feature importance analysis.
            df (dataframe): dataframe with features obtained from the transformation module.
        returns:
    """


    ## Eliminating columns (features) that were not selected in the importance analysis.
    nr_f = [col for col in df.columns if col not in fe_cln_dict]
    df.drop(nr_f, axis=1, inplace=True)

    ## Grouping all the non relevant categories of a relevant feature in specified tag.
    for cat_key in [key for key in fe_cln_dict if fe_cln_dict[key]["data_type"] == "categoric"]:
        m1 = ~df[cat_key].isin(fe_cln_dict[cat_key]["important_categories"])
        df.loc[m1, [cat_key]] = tag_non_relevant_cats


    return df





"------------------------------------------------------------------------------"
###################################
## Feature engineering functions ##
###################################


## Creating new features relevant for our model.
def feature_generation(df):
    """
    Creating new features relevant for our model.
        args:
            df (dataframe): df that will be engineered.
        returns:
            df (dataframe): resulting df with new features.
    """


    ## Separating features from labels
    df_features = df.drop("label", axis=1).copy()
    df_labels = df["label"].copy()


    ## Cleaning features to leave only those relevant for the model.
    nr_features_cols = [key for key in data_dict if
                (data_dict[key]['relevant']==True) &
                (data_dict[key]['model_relevant']==False)
              ]
    df_features.drop(nr_features_cols, axis=1, inplace=True)
    features_cols = list(df_features.columns)
    print("\n++ Complete list of features ({}) that will be fed to the model:".format(len(features_cols)))
    for col in features_cols:
        print("    {}. {}".format(features_cols.index(col) + 1, col))


    ## Generation dummy columns of categoric variables with OneHotEncoder

    #### Creating list of the features that will processed through the pipeline (categoric).
    cat_features_orig = [key for key in data_dict if
                    (data_dict[key]['model_relevant']==True) &
                    (data_dict[key]['data_type']=='categoric')
                   ]

    cat_features_add = [key for key in data_created_dict if
                    (data_created_dict[key]['model_relevant']==True) &
                    (data_created_dict[key]['data_type']=='categoric')
                   ]
    cat_features = cat_features_orig + cat_features_add

    print("\n++ List of categorical features ({}) that will be processed through the categoric pipeline are:".format(len(cat_features)))
    ohe_dict = {}
    for cat in cat_features:
        print("    {}. {}".format(cat_features.index(cat) + 1, cat))
        cat_list = list(df[cat].unique())
        cat_list.sort()
        ohe_dict[cat] = cat_list

    #### Creating list of the features that will processed through the pipeline (numeric).
    num_features_orig = [key for key in data_dict if
                    (data_dict[key]['model_relevant']==True) &
                    (data_dict[key]['data_type']=='numeric')
                   ]
    num_features_add = [key for key in data_created_dict if
                    (data_created_dict[key]['model_relevant']==True) &
                    (data_created_dict[key]['data_type']=='numeric')
                   ]
    num_features = num_features_orig + num_features_add

    print("\n++ List of numeric features ({}) that will be processed through the numeric pipeline are:".format(len(num_features)))
    for num in num_features:
        print("    {}. {}".format(num_features.index(num) + 1, num))


    #### Applying data processing pipeline.
    pipeline = ColumnTransformer([
        ('categoric', categoric_pipeline, cat_features),
        ('numeric', numeric_pipeline, num_features)
    ])
    df_features_prc = pipeline.fit_transform(df_features)
    print("\n++ Dimensions of matrix after going through pipeline: {}\n".format(df_features_prc.shape))


    ## List of all features that were fed to the model.
    df_features_prc_cols = list(df_features.columns)
    for ohe_key in ohe_dict:
        for i in range(len(ohe_dict[ohe_key])):
            df_features_prc_cols.insert(i + df_features_prc_cols.index(ohe_key), ohe_dict[ohe_key][i])
        df_features_prc_cols.remove(ohe_key)

    # print("\n++ ohe_dict: {}\n", ohe_dict)
    # print("\n++ df_features_prc_cols: {}\n", df_features_prc_cols)


    return df_features_prc, df_labels, ohe_dict, df_features_prc_cols




## Select most relevant features for the model.
def feature_selection(df, df_features_prc, df_labels, df_features_prc_cols, ohe_dict):
    """
    Select most relevant features for the model.
        args:
            df_features_prc (dataframe): Xxx
            df_labels (dataframe): Xxx
        returns:
            Xxx
    """


    ## Splitting data in train and test
    X_train, X_test, y_train, y_test = train_test_split(df_features_prc, df_labels, test_size=0.3)


    ## Selecting and training model - Random Forrest.
    model = models_dict["random_forest"]["model"]
    print("\n++ The model that will be used is: {}\n".format(model))


    ## Grid search CV to select best possible model.
    grid_search = GridSearchCV(model,
                               models_dict["random_forest"]["param_grid"],
                               cv=TimeSeriesSplit(n_splits=time_series_splits),
                               scoring=evaluation_metric,
                               return_train_score=True,
                               n_jobs=-1
                               )

    grid_search.fit(X_train, y_train)

    print("\n++ Grid search results:\n")
    print("    ++++ Best parameters: {}".format(grid_search.best_params_))
    print("    ++++ Best estimator: {}".format(grid_search.best_estimator_))
    print("    ++++ Number of features in best estimator: {} \n".format(grid_search.best_estimator_.n_features_))
    print("    ++++ Best estimator score: {}\n".format(grid_search.best_score_))
    print("    ++++ Best estimator oob score: {}\n".format(grid_search.best_estimator_.oob_score_))


    ## Determining model's best estimators
    feature_importance = pd.DataFrame(
        {
            "Importance": grid_search.best_estimator_.feature_importances_,
            "Feature": df_features_prc_cols
        }
    )
    #### Enhancing the contents of the feature importance dataframe.
    feature_importance.sort_values(by="Importance", ascending=False, inplace=True)
    feature_importance.reset_index(inplace=True, drop=True)
    feature_importance["Mother_feature"] = feature_importance["Feature"].apply(lambda x: find_mother(x, ohe_dict))
    feature_importance["Important"] = feature_importance["Importance"].apply(lambda x: True if x >= feature_importance_theshold else False)

    print("\n++ Threshold used to discriminate variables' importance: {}\n".format(feature_importance_theshold))
    print("\n++ Report of processed features importance analysis: \n{}\n".format(feature_importance))


    ## Summary dictionary derived from feature importance analysis.
    fe_cln_dict = feature_cleaning_dict(feature_importance, ohe_dict)
    print("\n++ Summary dictionary derived from feature importance analysis: \n")
    print("\n{}\n".format(json_dump_dict(fe_cln_dict)))


    ## Cleaning the feature dataframe based on the feature importance analysis.
    df = apply_feature_selection(fe_cln_dict, df)


    ## Processing selected features through pipeline
    cat_imp_features = [key for key in fe_cln_dict if fe_cln_dict[key]["data_type"] == "categoric"]
    num_imp_features = [key for key in fe_cln_dict if fe_cln_dict[key]["data_type"] == "numeric"]
    pipeline = ColumnTransformer([
        ('categoric', categoric_pipeline, cat_imp_features),
        ('numeric', numeric_pipeline, num_imp_features)
    ])
    df_imp_features_prc = pipeline.fit_transform(df)
    print("\n++ Dimensions of matrix after going through pipeline: {}\n".format(df_imp_features_prc.shape))


    return df_imp_features_prc





"------------------------------------------------------------------------------"
#######################################
## Feature engineering main function ##
#######################################


## Function desigend to execute all fe functions.
def feature_engineering(transformation_pickle_loc, fe_pickle_loc_imp_features, fe_pickle_loc_feature_labs):
    """
    Function desigend to execute all fe functions.
        args:
            transformation_pickle_loc (string): path where the picke obtained from the transformation is.
            fe_pickle_loc (string): location where the resulting pickle object will be stored.
        returns:
            -
    """

    ## Executing transformation functions
    df = load_transformation(transformation_pickle_loc)
    # df_features_prc, df_labels, df_features_prc_cols = feature_generation(df)
    df_features_prc, df_labels, ohe_dict, df_features_prc_cols = feature_generation(df)
    # df_features_prc = feature_selection(df_features_prc, df_labels, df_features_prc_cols)
    df_imp_features_prc = feature_selection(df, df_features_prc, df_labels, df_features_prc_cols, ohe_dict)
    # save_fe(df_features_prc, fe_pickle_loc)
    save_fe(df_imp_features_prc, fe_pickle_loc_imp_features)
    save_fe(df_labels, fe_pickle_loc_feature_labs)
    print("\n** Feature engineering module successfully executed **\n")





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
