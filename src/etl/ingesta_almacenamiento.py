#!/usr/bin/env python





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports

from datetime import (date, datetime)

import sys

import yaml

import pickle

import re

import unicodedata

import os



## Third party imports

from sodapy import Socrata

import boto3

import pandas as pd


## Local application imports


from src.utils.params_gen import (
    regex_violations,
    serious_viols,
)

from src.utils.utils import (
    save_df,
    load_df,
    read_yaml_file,
    get_s3_credentials,
    get_s3_resource,
    get_api_token
)

from src.utils.data_dict import data_dict

from src.utils.params_gen import (
    bucket_name,
    local_temp_ingestions,
    year_dir,
    month_dir,
    hist_ingest_path,
    hist_dat_prefix,
    cont_ingest_path,
    cont_dat_prefix,
    today_info,
)





"------------------------------------------------------------------------------"
#######################################
## Data loading and saving functions ##
#######################################


##
def get_client(token):
    return Socrata("data.cityofchicago.org", token)



##
def ingesta_inicial(client, limit=300000):
    return client.get("4ijn-s7e5", limit=limit)



##
def ingesta_consecutiva(client, soql_query):
    return client.get("4ijn-s7e5", where=soql_query)



## Getting most recent date in local to download consecutive
def most_recent_lcl_for_cons():

    ## List of all years
    lyrs = [ydir[-4:] for ydir in os.listdir(local_temp_ingestions + "consecutive") if year_dir in ydir]


    ## Intermediary function to get most recent date to call API for data
    def get_date_by_cases(ing_date_ref):

        ## Getting most recent year in local directories based in ingestion type
        mr_yr = max([ydir[-4:] for ydir in os.listdir(local_temp_ingestions + ing_date_ref) if year_dir in ydir])

        ## Most recent month
        new_path = local_temp_ingestions + ing_date_ref + "/" + year_dir + mr_yr
        mr_mth = max([mdir[-2:] for mdir in os.listdir(new_path) if month_dir in mdir])

        ## List of all ingestions in most recent dates
        new_path = local_temp_ingestions + ing_date_ref + "/" + year_dir + mr_yr + "/" + month_dir + mr_mth
        lings = [ing for ing in os.listdir(new_path)]

        ## Regular expression to find all dates in list of ingestions
        if ing_date_ref == "consecutive":
            regex = cont_dat_prefix + "(.*).pkl"
        elif ing_date_ref == "initial":
            regex = hist_dat_prefix + "(.*).pkl"
        else:
            raise NameError('No reference to perform regex')

        ## Most recent date of all ingestions
        most_recent_ing = max([re.search(regex, ing).group(1) for ing in lings if ".pkl" in ing])

        return most_recent_ing


    ## Case when we do have other consecutives stored locally
    if len(lyrs) > 0:
        print("**** Consecutive pickles found, therefore downloading data based on most recent consecutive pickle")
        ing_date_ref = "consecutive"
        most_recent_ing = get_date_by_cases(ing_date_ref)

    ## Case when we don't have any historic ingestions
    elif len(lyrs) == 0:
        print("**** Consecutive pickles NOT found, therefore downloading data based on historic pickle")
        ing_date_ref = "initial"
        most_recent_ing = get_date_by_cases(ing_date_ref)

    ## Anomaly in algorithm
    else:
        raise NameError('Invalid case looking for pickles.')

    return most_recent_ing



## Saving data donwloaded with Chicago's API
def guardar_ingesta(ingest_type, bucket_name):
    """
    Saving data donwloaded with Chicago's API
    :param ingest_type:
    :param bucket_name:
    :return:
    """

    ## Getting s3 resource to store data in s3.
    s3 = get_s3_resource()

    ## Read token from credentials file
    token = get_api_token("conf/local/credentials.yaml")

    ## Getting client to download data with API
    client = get_client(token)


    ## Downloading data and storing it temporaly in local machine prior upload to s3
    if ingest_type == "initial":

        ## Requesting all data from API
        ingesta = ingesta_inicial(client)

        create_path_ingestion(ingest_type)


    elif ingest_type == "consecutive":

        ## Finding most recent date in consecutive pickles
        pkl_mrd = most_recent_lcl_for_cons()
        print("**** Consecutive data will be downloaded from {} ****".format(pkl_mrd))
        print("********")

        create_path_ingestion(ingest_type)

        ## Building query to download data of interest
        soql_query = "inspection_date >= '{}'".format(pkl_mrd)

        ingesta = pickle.dumps(ingesta_consecutiva(client, soql_query))


    else:
        raise NameError('Invalid parameter')


    return ingesta



## (test function) Converting data into pandas dataframe by providing the data's location.
def ingest_local_csv(data_path):
    """
    (test function) Converting data into pandas dataframe by providing the data's location.
        args:
            filename (string): path relative to the repo's root where the data (.csv file) is located
        returns:
            df_c5 (dataframe): .csv file converted to pandas dataframe
    """

    ## Reading file in specified path
    df = pd.read_csv(data_path)

    return df



## Saving dataframe as pickle object in specified location.
def save_ingestion(df, path):
    """
    Saving dataframe as pickle object in specified location.
        args:
            df (dataframe): df that will be converted and saved as pickle.
            path (string): location where the pickle will be stored.
    """

    ## Converting and saving dataframe.
    save_df(df, path)



## Creating path to store ingestion
def create_path_ingestion(ingest_type):
    """
    Creating path to store ingestion

    :param ingest_type:
    :return:
    """

    #### Variables
    year_str = year_dir + today_info[:4]
    month_str = month_dir + today_info[5:7]


    #### Creating year directory
    local_temp_ing_year = local_temp_ingestions + ingest_type + "/" + year_str + "/"
    if year_str not in os.listdir(local_temp_ingestions + ingest_type):
        os.mkdir(local_temp_ing_year)


    #### Creating month directory
    local_temp_ing_year_month = local_temp_ing_year + month_str + "/"
    if month_str not in os.listdir(local_temp_ing_year):
        os.mkdir(local_temp_ing_year_month)



## Saving ingestion locally
def save_local_ingestion(ingest_type):
    """
    Saving ingestion locally

    :param ingest_type:
    :return:
    """

    ## Name of new directory where latest ingestion will be stored
    year_str = year_dir + today_info[:4]
    month_str = month_dir + today_info[5:7]
    local_temp_ing_year_month = local_temp_ingestions + ingest_type + "/" + year_str + "/" + month_str + "/"

    ## Saving temporal ingestion locally based on initial parameters
    if ingest_type == 'initial':
        local_save_loc = local_temp_ing_year_month + hist_dat_prefix + today_info + ".pkl"

    elif ingest_type == 'consecutive':
        local_save_loc = local_temp_ing_year_month + cont_dat_prefix + today_info + ".pkl"

    else:
        raise NameError('Invalid parameter')

    return local_save_loc





"------------------------------------------------------------------------------"
#############################
## Data cleaning functions ##
#############################


## Transform columns' names to standard format
def clean_col_names(dataframe):
    """
    Transform columns' names to standard format (lowercase, no spaces, no points)
        args:
            dataframe (dataframe): df whose columns will be formatted.
        returns:
            dataframe (dataframe): df with columns cleaned.
    """

    ## Definition of cleaning funcitons that will be applied to the columns' names
    fun1 = lambda x: re.sub('[^a-zA-Z0-9 \n\.]', '-', x.lower()) ## change special characters for "-"
    fun2 = lambda x: unicodedata.normalize("NFD", x).encode("ascii", "ignore").decode("utf-8") ## substitute accents for normal letters
    fun3 = lambda x: re.sub(' ', '_', x.lower()) ## change spaces for "_"

    funcs = [fun1, fun2, fun3]

    ## Applying the defined functions to the columns' names
    for fun in funcs:
        dataframe.columns = [fun(col) for col in dataframe.columns]

    return dataframe



## Converting observatios for selected columns into lowercase
def convert_lower(data, vars_lower):
    """
     Converting observatios for selected columns into lowercase
        args:
            data (dataframe): data that is being analyzed.
            vars_lower (list): list of the columns' names in the dataframe that will be changed to lowercase.
        returns:
            data(dataframe): dataframe that is being analyzed with the observations (of the selected columns) in lowercase.
    """
    for x in vars_lower:
        data[x]=data[x].str.lower()
    return data



## Cleaning columns with text
def clean_strings(txt):
    """
    Cleaning columns with text

    :param txt: text entry that wiil be cleaned
    :type txt: string

    :return txt: cleaned text entry
    :type txt: string
    """

    ## Eliminating unnecessary whitespace
    txt = txt.strip()

    ## Substitute accents for normal letters
    txt = unicodedata.normalize("NFD", txt).encode("ascii", "ignore").decode("utf-8")

    ## Setting everything to lowercase and substituting special characters with "-"
    txt = re.sub('[^a-zA-Z0-9 \n\.]', '-', txt.lower())

    ## Changing spaces with "_"clean_col_names
    txt = re.sub(" ", "_", txt)

    return txt



## Create new column on working dataframe to discriminate false from true calls.
def generate_label(df):
    """
    Create new column on working dataframe to discriminate false from true calls.
        args:
            df (dataframe): df where the new column will be added.
        returns:
            -
    """

    ## Identifying feature that will serve as predictive label
    predict_label = [key for key in data_dict if "predict_label" in data_dict[key]][0]

    ## Crating new label column,
    df["label"] = df[predict_label].apply(lambda x: 1 if "pass" in x else 0)


    return df



## Eliminating unused columns from dataframe.
def drop_cols(df):
    """
    Eliminating unused columns from dataframe.
        args:
            df (dataframe): df that will be cleaned.
        returns:
            -
    """

    ## Obtainig list of columns that are relevant
    nrel_col = [col for col in data_dict if data_dict[col]["relevant"] == False]

    ## Dropping non relevant columns
    df.drop(nrel_col, inplace=True, axis=1)


    return df



## Master cleaning function: initial function to clean the dataset. This function uses the functions above.
def initial_cleaning(data):
    """
    Master cleaning function: initial function to clean the dataset. This function uses the functions above.

    :param data: raw dataframe that will be go through the initial cleaning process

    :return dfx: resulting dataframe after initial cleaning
    """

    ## Creating copy of initial dataframe
    dfx = data.copy()

    ## Cleaning names of columns
    clean_col_names(dfx)

    ## Eliminating unused columns
    dfx = drop_cols(dfx)

    ## Cleaning string columns
    #### Selecting only columns that are relevant and that have strings
    str_cols = [feat for feat in data_dict if (data_dict[feat]["relevant"] == True) & (data_dict[feat]["data_type"] == "string")]
    #### Making sure that these columns are strings and cleaning the strings in the column
    for str_col in str_cols:
        dfx[str_col] = dfx[str_col].astype("str")
        dfx[str_col] = dfx[str_col].apply(lambda x: clean_strings(x))

    ## Adding column with predictive label
    dfx = generate_label(dfx)

    return dfx





"------------------------------------------------------------------------------"
###############################
## Ingestion master function ##
###############################


## Function desigend to execute all ingestion functions.
def ingest(data_path, ingestion_pickle_loc):
    """
    Function desigend to execute all ingestion functions.
        args:
            path (string): path where the project's data is stored.
            ingestion_save (string): location where the resulting pickle object will be stored.
        returns:
            -
    """

    ## Executing ingestion functions
    df = ingest_local_csv(data_path) ## Temporal function
    # guardar_ingesta(bucket_name, bucket_path)
    df = initial_cleaning(df)
    save_ingestion(df, ingestion_pickle_loc) ## Temporal function
    print("\n** Ingestion module successfully executed **\n")





"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
