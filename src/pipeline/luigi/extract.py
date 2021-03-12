
import luigi
from datetime import (date, datetime)
import pickle
import re
from sodapy import Socrata
import boto3
import yaml
import sys
import joblib



## local imports
from src.utils.general import (
	read_yaml_file,
	get_s3_credentials,
	get_api_token
)

## deberia estar en UTILS:
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



def get_client(token):
    return Socrata("data.cityofchicago.org", token)

def ingesta_inicial(client, limit=300000):
    return client.get("4ijn-s7e5", limit=limit)

def ingesta_consecutiva(client, soql_query):
    return client.get("4ijn-s7e5", where=soql_query)


## Parameters
## AWS parameters
bucket_name = "data-product-architecture-equipo-9"

hist_ingest_path = "ingestion/initial/"
hist_dat_prefix = "historic-inspections-"

cont_ingest_path = "ingestion/consecutive/"
cont_dat_prefix = "consecutive-inspections-"

## Naming files
today_info = date.today().strftime('%Y-%m-%d')

###########

class APIDataIngestion(luigi.Task):

    ## luigi. parameter... /// se lo envia el task siguiente ... initial consecutive
    ingest_type = luigi.Parameter()



    def output(self):
        # guardamos en archivo local para que qeude registro de que se ejecuto el task
        return luigi.local_target.LocalTarget('src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl')

    def run(self):
        ## Getting client to download data with API

        token = get_api_token("conf/local/credentials.yaml")
        client = get_client(token)

        if self.ingest_type =='initial':
            ingesta = ingesta_inicial(client)

        elif self.ingest_type=='consecutive':

			## Getting s3 resource to store data in s3.
			#s3= get_s3_resource()

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

        output_file= open(self.output().path, 'wb')
        pickle.dump(ingesta, output_file)
