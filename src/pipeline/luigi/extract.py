## MODULE TO EXTRACT DATA FROM API





"----------------------------------------------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports

from datetime import (date, datetime)

import pickle

import re

import yaml


## Third party imports

import luigi

import joblib


## Local application imports

from src.utils.general import (
    # read_yaml_file,
    # get_s3_credentials,
    get_api_token
)

from src.utils.utils import (
    get_s3_resource,
    get_client,
    ingesta_inicial,
    ingesta_consecutiva,
)

from src.utils.params_gen import (
    bucket_name,
    hist_ingest_path,
    hist_dat_prefix,
    cont_ingest_path,
    cont_dat_prefix,
)





"----------------------------------------------------------------------------------------------------------------------"
################
## Parameters ##
################


## Naming files
today_info = date.today().strftime('%Y-%m-%d')





"----------------------------------------------------------------------------------------------------------------------"
################
## Luigi task ##
################


## Task aimed to download data from API
class APIDataIngestion(luigi.Task):


    ## Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    ## Run: download data from API depending on the ingestion type
    def run(self):

        ## Getting client to request data from API
        token = get_api_token("conf/local/credentials.yaml")
        client = get_client(token)

        ## Requesting all data from API
        if self.ingest_type == 'initial':
            ingesta = ingesta_inicial(client)


        elif self.ingest_type == 'consecutive':

            ## Getting s3 resource to store data in s3.
            s3 = get_s3_resource()


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

        ## Saving ingestion results
        pickle.dump(ingesta, open(self.output().path, 'wb'))


    ## Output: storing downloaded information locally
    def output(self):

        ## Saving temporal ingestion locally based on initial parameters
        if self.ingest_type == 'initial':
            pkl_name = hist_dat_prefix + today_info + ".pkl"
            return luigi.local_target.LocalTarget('src/pipeline/luigi/ingestion_tmp/initial/' + pkl_name)

        elif self.ingest_type == 'consecutive':
            pkl_name = cont_dat_prefix + today_info + ".pkl"
            return luigi.local_target.LocalTarget('src/pipeline/luigi/ingestion_tmp/consecutive/' + pkl_name)





"----------------------------------------------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"----------------------------------------------------------------------------------------------------------------------"