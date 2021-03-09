## MODULE TO TRANSFORM DATA BASED ON DATA-TYPES AND SAVE RESULTS AS PICKLE





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Python libraries

import pickle

import sys

import pandas as pd

import numpy as np


## Ancillary modules

from src.utils.data_dict import (
    data_created_dict,
    data_dict
)

from src.utils.utils import (
    update_data_created_dict,
    load_df,
    save_df
)

from src.utils.params import (
    ingestion_pickle_loc,
    transformation_pickle_loc
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

    update_data_created_dict("anio_inicio", relevant=True, data_type="categoric", model_relevant=True)

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
        div= 30.4
    elif "mes" in col:
        div=12

    ## Creating cyclical variables.
    df[col + '_sin'] = np.sin(2*np.pi*df[col].apply(lambda x: float(x))/div)
    df[col + '_cos'] = np.cos(2*np.pi*df[col].apply(lambda x: float(x))/div)


    ## Updating data creation dictionary to include cyclical features.
    update_data_created_dict(col + "_sin", relevant=True, data_type="numeric", model_relevant=True)
    update_data_created_dict(col + "_cos", relevant=True, data_type="numeric", model_relevant=True)



    return df



## Conduct relevant transformations to numeric variables.
def numeric_tranformation(col, df):
    """
    Conduct relevant transformations to numeric variables.
        args:
            col (string): name of numeric column that will be transformed.
            df (dataframe): df that will be transformed.
        returns:
            df (dataframe): resulting df with cleaned numeric column.
    """

    pass



## Conduct relevant transformations to categoric variables.
def categoric_trasformation(col, df):
    """
    Conduct relevant transformations to categoric variables.
        args:
            col (string): name of categoric column that will be transformed.
            df (dataframe): df that will be transformed.
        returns:
            df (dataframe): resulting df with cleaned categoric column.
    """

    df[col] = df[col].replace(['LLAMADA DEL 911'],'LLAMADA_911_066')
    df[col] = df[col].replace(['LLAMADA DEL 066'],'LLAMADA_911_066')

    return df





"------------------------------------------------------------------------------"
####################################
## Transformation master function ##
####################################


## Function desigend to execute all transformation functions.
def transform(ingestion_pickle_loc, transformation_pickle_loc):
    """
    Function desigend to execute all transformation functions.
        args:
            ingestion_pickle_loc (string): path where the picke obtained from the ingestion is.
            transformation_pickle_loc (string): location where the resulting pickle object will be stored.
        returns:
            -
    """

    ## Executing transformation functions
    df = load_ingestion(ingestion_pickle_loc)
    df = date_transformation("fecha_creacion", df)
    df = hour_transformation("hora_creacion", df)
    # df = numeric_tranformation(col, df)
    df = categoric_trasformation("tipo_entrada", df)
    # print(data_created_dict)
    save_transformation(df, transformation_pickle_loc)
    print("\n** Tranformation module successfully executed **\n")





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
