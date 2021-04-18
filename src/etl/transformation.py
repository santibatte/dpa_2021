## MODULE TO TRANSFORM DATA BASED ON DATA-TYPES AND SAVE RESULTS AS PICKLE





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports

import pickle

import sys

import re

from datetime import (date, datetime)


## Third party imports

import pandas as pd

import numpy as np


## Local application imports

from src.utils.data_dict import (
    data_created_dict,
    data_dict
)

from src.utils.utils import (
    update_created_dict,
    load_df,
    save_df,
    write_csv_from_df,
)

from src.utils.params_gen import (
    metadata_dir_loc,

    ingestion_pickle_loc,
    transformation_metadata,
    transformation_metadata_index,
    trans_count,
    trans_metadata_csv_name,
    cat_reduction_ref,

    regex_violations,
    serious_viols,
)





"------------------------------------------------------------------------------"
#################################
## Generic ancillary functions ##
#################################


## Loading ingestion pickle as dataframe for transformation pipeline.
def load_ingestion(path):
    """
    Loading ingestion pickle as dataframe for transformation pipeline.
        args:
            path (string): location where the pickle that will be loaded is.
        returns:
            df (dataframe): dataframe obtained from ingestion pickle.
    """

    df = load_df(path)

    return df



## Save transformed data frame as pickle.
def save_transformation(df, path):
    """
    Save transformed data frame as pickle.
        args:
            df (dataframe): transformation resulting dataframe.
            path (string): location where the pickle object will be stored.
        returns:
            -
    """

    save_df(df, path)



## Identify if any serious violations were committed if
def mark_serious_violations(row):
    """
    Identify if any serious violations were committed if

    :param row: dataframe row where violations codes to be evaluated are present.

    :return:
    """

    res = "no_result"

    v_nums = re.findall(regex_violations, row)

    if (len(set(serious_viols) - set(v_nums)) == len(set(serious_viols))) & (len(v_nums) != 0):
        res = "no_serious_violations"

    elif len(set(serious_viols) - set(v_nums)) != len(set(serious_viols)):
        res = "serious_violations"

    return res



## Function to reduce categories of selected column
def cat_red(row, dfcol):
    """
    Function to reduce categories of selected column

    :param row: row in dataframe that contains the category that will be evaluated
    :type row: string

    :param dfcol: name of the column processed
    :type dfcol: string

    :return res: string from row simplified in predefined category
    :type res: string
    """
    res = str(dfcol) + "_other"

    for cat in cat_reduction_ref[dfcol]:

        for key_word in cat_reduction_ref[dfcol][cat]["key_words"]:

            if key_word in row:
                res = cat_reduction_ref[dfcol][cat]["substitution"]
                break

    return res





"------------------------------------------------------------------------------"
##############################
## Transformation functions ##
##############################


## Conduct relevant transformations to date variables.
def date_transformation(col, df):
    """
    Conduct relevant transformations to date variables.
        args:
            col (string): name of date column that will be transformed.
            df (dataframe): df that will be transformed.
        returns:
            df (dataframe): resulting df with cleaned date column.
    """
    fechas_inicio = df[col].str.split("/", n=2, expand=True)
    df['dia_inicio'] = fechas_inicio[0]
    df['mes_inicio'] = fechas_inicio[1]
    df['anio_inicio'] = fechas_inicio[2]

    df['anio_inicio'] = df['anio_inicio'].replace(['19'],'2019')
    df['anio_inicio'] = df['anio_inicio'].replace(['18'],'2018')

    cyc_lst = ["dia_inicio", "mes_inicio"]
    for val in cyc_lst:
        cyclic_trasformation(df, val)

    df.drop(['dia_inicio','mes_inicio'], axis=1, inplace=True)

    update_data_created_dict("anio_inicio", relevant=True, feature_type="categoric", model_relevant=True)


    ## Updating transformation counts
    global trans_count
    trans_count += 1


    return df



##Conduct relevant transformations to hour variables.
def hour_transformation(col, df):
    """
    """

    df[col] = pd.to_timedelta(df[col],errors='ignore')
    hora_inicio = df[col].str.split(":", n=2,expand=True)
    df['hora_inicio'] = hora_inicio[0]
    df['min_inicio'] = hora_inicio[1]
    df['hora_inicio'] = round(df['hora_inicio'].apply(lambda x: float(x)),0)

    cyc_lst = ["hora_inicio"]
    for val in cyc_lst:
        cyclic_trasformation(df, val)

    df.drop(['hora_inicio','min_inicio'], axis=1, inplace=True)


    ## Updating transformation counts
    global trans_count
    trans_count += 1


    return df



def cyclic_trasformation(df, col):
    """
    Conduct relevant cyclical hour transformations to cos and sin coordinates.
        args:
            col (string): name of  column that will be transformed into cyclical hr.
            df (dataframe): df that will be transformed.
        returns:
            df (dataframe): resulting df with cyclical column.
    """


    ## Specifying divisors related to time.
    if "hora" in col:
        div=24
    elif "dia" in col:
        div=30.4
    elif "mes" in col:
        div=12

    ## Creating cyclical variables.
    df[col + '_sin'] = np.sin(2*np.pi*df[col].apply(lambda x: float(x))/div)
    df[col + '_cos'] = np.cos(2*np.pi*df[col].apply(lambda x: float(x))/div)


    ## Updating data creation dictionary to include cyclical features.
    update_created_dict(col + "_sin", relevant=True, feature_type="numeric", model_relevant=True)
    update_created_dict(col + "_cos", relevant=True, feature_type="numeric", model_relevant=True)


    ## Updating transformation counts
    global trans_count
    trans_count += 1


    return df



## Create new column with info regarding serious violations
def serious_viols_col(df):
    """
    Create new column with info regarding serious violations

    :param df: raw dataframe that will be go through the initial cleaning process

    :return dfx: resulting dataframe after initial cleaning
    """

    ## Adding specific string to beggining of `violations`
    df["violations"] = "-_" + df["violations"]

    ## Creating new column with label regarding presence of serious violations
    df["serious_violations"] = df["violations"].apply(lambda x: mark_serious_violations(x))

    ## Updating data creation dictionary to new column
    update_created_dict("serious_violations", relevant=True, feature_type="categoric", model_relevant=True)


    ## Updating transformation counts
    global trans_count
    trans_count += 1


    return df



## Reducing the number of categories in selected columns
def category_reductions(df):
    """
    Reducing the number of categories in selected columns

    :param df: dataframe whose categoric columns will be processed
    :type df: dataframe

    :return df: processed dataframe
    :type df: dataframe
    """

    for dfcol in cat_reduction_ref:
        df[dfcol] = df[dfcol].apply(lambda x: cat_red(x, dfcol))


    ## Updating transformation counts
    global trans_count
    trans_count += 1


    return df





"------------------------------------------------------------------------------"
####################################
## Transformation master function ##
####################################


## Function desigend to execute all transformation functions.
#def transform(ingestion_pickle_loc, transformation_pickle_loc):
def transform(df,transformation_pickle_loc):
    """
    Function desigend to execute all transformation functions.
        args:
            df: data frame ingestion
            #ingestion_pickle_loc (string): path where the pickle obtained from the ingestion is.
            transformation_pickle_loc (string): location where the resulting pickle object will be stored.
        returns:
            -
    """

    ## Storing time execution metadata
    transformation_metadata[transformation_metadata_index] = str(datetime.now())


    ## Executing transformation functions

    #### Loading data
    df = load_ingestion(ingestion_pickle_loc)

    #### List of df's original set of columns
    orig_cols = df.columns

    #### Adding column of serious violations (transformation)
    df = serious_viols_col(df)

    #### Reducing the number of categories in data (transformation)
    df = category_reductions(df)

    #### List of df's resulting set of columns
    res_cols = df.columns


    ## Saving results
    save_transformation(df, transformation_pickle_loc)


    ## Working with module's metadata

    #### Storing number of transformations in metadata
    transformation_metadata["trans_count"] = trans_count

    #### String with list of new columns added after transformation (pipe separated)
    transformation_metadata["new_cols"] = " | ".join(set(res_cols) - set(orig_cols))

    #### Converting metadata into dataframe and saving locally
    df_meta = pd.DataFrame.from_dict(transformation_metadata, orient="index").T
    df_meta.set_index(transformation_metadata_index, inplace=True)
    write_csv_from_df(df_meta, metadata_dir_loc, trans_metadata_csv_name)


    print("\n** Tranformation module successfully executed **\n")

    return df






"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
