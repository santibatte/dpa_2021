#!/usr/bin/env python





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports

import yaml





"------------------------------------------------------------------------------"
###############
## Functions ##
###############


## Load yaml cofigurations
def read_yaml_file(yaml_file):
    """
    Load yaml cofigurations
    """

    config = None
    try:
        with open (yaml_file, 'r') as f:
            config = yaml.safe_load(f)
    except:
        raise FileNotFoundError('Couldnt load the file')

    return config



## Get s3 credentials
def get_s3_credentials(credentials_file):
    """
    Get s3 credentials
    """
    credentials = read_yaml_file(credentials_file)
    s3_creds = credentials['s3']

    return s3_creds

## Get API TOKEN
def get_api_token(credentials_file):
    """
    Get api token 
    """
    token=read_yaml(credentials_file)['food_inspections']
    return token
