#!/usr/bin/env python





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports

from datetime import date

import sys

import yaml

import pickle



## Third party imports

from sodapy import Socrata

import boto3

import pandas as pd


## Local application imports

# sys.path.append("../")

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
bucket_name = "data-product-architecture-equipo-super"

hist_ingest_path = "ingestion/initial/"
hist_dat_prefix = "historic-inspections-"

cont_ingest_path = "ingestion/consecutive/"
cont_dat_prefix = "consecutive-inspections-"

## Naming files
today_info = date.today().strftime('%Y-%m-%d')





"------------------------------------------------------------------------------"
###############
## Functions ##
###############


##
def get_client(token):
	return Socrata("data.cityofchicago.org", token)


##
def ingesta_inicial(client, limit=300000):
	return client.get("4ijn-s7e5", limit=limit)



##
def ingesta_consecutiva(client, limit=1000):
	return client.get("4ijn-s7e5", limit=limit)



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
		pkl_temp_local_path = "data/" + hist_ingest_path + hist_dat_prefix + today_info + ".pkl"
		ingesta=pickle.dumps(ingesta_inicial(client))
		file_name = hist_dat_prefix + today_info + ".pkl"


	elif "consecutive" in bucket_path:
		pkl_temp_local_path = "data/" + cont_ingest_path + cont_dat_prefix + today_info + ".pkl"
		ingesta=pickle.dumps(ingesta_consecutiva(client))
		file_name = cont_dat_prefix + today_info + ".pkl"

	else:
		raise NameError('Unknown bucket path')

	## Uploading data to s3
	return s3.put_object(Bucket= bucket_name, Key=bucket_path + file_name, Body=ingesta)







"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
