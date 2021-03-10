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



## Third party imports

from sodapy import Socrata

import boto3

import pandas as pd


## Local application imports

from src.utils.general import (
	read_yaml_file,
	get_s3_credentials,
	get_api_token
)





"------------------------------------------------------------------------------"
################
## Parameters ##
################


## AWS parameters
bucket_name = "data-product-architecture-equipo-9"

hist_ingest_path = "ingestion/initial/"
hist_dat_prefix = "historic-inspections-"

cont_ingest_path = "ingestion/consecutive/"
cont_dat_prefix = "consecutive-inspections-"

## Naming files
today_info = date.today().strftime('%Y-%m-%d')





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



## Getting an s3 resource to interact with AWS s3 based on .yaml file
def get_s3_resource():
	"""
	Getting an s3 resource to interact with AWS s3 based on .yaml file
		args:
			-
		returns:
			s3 (aws client session): s3 resource
	"""

	s3_creds = get_s3_credentials("conf/local/credentials.yaml")

	session = boto3.Session(
	    aws_access_key_id=s3_creds['aws_access_key_id'],
	    aws_secret_access_key=s3_creds['aws_secret_access_key']
	)

	s3 = session.client('s3')

	return s3



## Saving data donwloaded with Chicago's API
def guardar_ingesta(bucket_name, bucket_path):
	"""
	Saving data donwloaded with Chicago's API
		args:
			- bucket_name (string): name of bucket where data will be stored.
			- bucket_path (string): path within the bucket to store data.
			- pkl_path (string): string with location of temporal pkl stored in local machine.
	"""

	## Getting s3 resource to store data in s3.
	s3 = get_s3_resource()

	#Read token from credentials file
	token = get_api_token("conf/local/credentials.yaml")

	## Getting client to download data with API
	client = get_client(token)


	## Downloading data and storing it temporaly in local machine prior upload to s3
	if "initial" in bucket_path:
		ingesta = pickle.dumps(ingesta_inicial(client))
		file_name = hist_dat_prefix + today_info + ".pkl"


	elif "consecutive" in bucket_path:

		## Finding most recent date in consecutive pickles

		#### Getting list with pickles stored in s3 consecutive
		objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=cont_ingest_path)['Contents']

		#### Regular expression to isolate date in string
		regex = str(cont_dat_prefix) + "(.*).pkl"

		#### List of all dates in consecutive pickles
		pkl_dates = [datetime.strptime(re.search(regex, obj["Key"]).group(1), '%Y-%m-%d') for obj in objects if cont_dat_prefix in obj["Key"]]

		#### Consecutive pickle most recent date
		pkl_mrd = datetime.strftime(max(pkl_dates), '%Y-%m-%d')


		## Building query to download data of interest
		soql_query = "inspection_date >= '{}'".format(pkl_mrd)

		ingesta = pickle.dumps(ingesta_consecutiva(client, soql_query))
		file_name = cont_dat_prefix + today_info + ".pkl"


	else:
		raise NameError('Unknown bucket path')


	## Uploading data to s3
	return s3.put_object(Bucket=bucket_name, Key=bucket_path + file_name, Body=ingesta)



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
    fun1 = lambda x: x.lower() ## convert to lowercase
    fun2 = lambda x: re.sub("( |¡|!|¿|\?|\.|,|;|:)", "_", x) ## eliminate spaces and punctuation signs for underscore
    fun3 = lambda x: unicodedata.normalize("NFD", x).encode("ascii", "ignore").decode("utf-8") ## substitute accents for normal letters
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
def clean_txt(txt):
    """
    """

    ## Setting everything to lowercase and substituting special characters with "-"
    txt = re.sub('[^a-zA-Z0-9 \n\.]', '-', txt.lower())

    ## Changing spaces with "_"clean_col_names
    txt = re.sub(" ", "_", txt)


    return txt



## Identify if any serious violations were committed if
def mark_serious_violations(row):
    """
    Identify if any serious violations were committed if

    :param row: dataframe row where violations codes to be evaluated are present.

    :return:
    """
    try:

        v_nums = re.findall(r'\| (.+?). ', row)

        if len(set(serious_viols) - set(v_nums)) == len(set(serious_viols)):
            res = "no_serious_violations"

        else:
            res = "serious_violations"

    except:
        res = "no_result"

    return res



## Initial function to clean the dataset
def initial_cleaning(data):
    """
    Initial function to clean the dataset

    :param data: raw dataframe that will be go through the initial cleaning process

    :return dfx: resulting dataframe after initial cleaning
    """

    ## Creating copy of initial dataframe
    dfx = data.copy()


    ## Cleaning names of columns
    clean_col_names(dfx)


    ## Creating column identifying if entry has serious violations

    #### Adding specific string to beggining of `violations`
    dfx["violations"] = "| " + dfx["violations"]

    #### Creating new column with label regarding presence of serious violations
    dfx["serious_violations"] = dfx["violations"].apply(lambda x: mark_serious_violations(x))


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
    df = ingest_file(data_path)
    df = set_index(df)
    df = drop_cols(df)
    df = generate_label(df)
    save_ingestion(df, ingestion_pickle_loc)
    print("\n** Ingestion module successfully executed **\n")





"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
