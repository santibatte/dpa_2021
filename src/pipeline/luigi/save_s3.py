## MODULE WITH TASK TO SAVE EXTRACTIONS IN S3





"----------------------------------------------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports

import os

import pickle


## Third party imports

import luigi
import luigi.contrib.s3

import boto3


## Local application imports

from src.pipeline.luigi.extract import APIDataIngestion

from src.utils.utils import (
    get_s3_resource,
    get_s3_resource_luigi,
    get_key,
)

from src.utils.params_gen import (
    local_temp_ingestions,
    year_dir,
    month_dir,
    hist_dat_prefix,
    cont_dat_prefix,
    today_info,
)

def path_file_fn(ingest_type):
    if ingest_type == 'initial':
        path_file_2 = hist_dat_prefix + today_info + ".pkl"
    elif ingest_type == 'consecutive':
        path_file_2 = cont_dat_prefix + today_info + ".pkl"
    return path_file_2





"----------------------------------------------------------------------------------------------------------------------"
################
## Luigi task ##
################


## Task aimed to store ingestion in s3
class S3Task(luigi.Task):


    ## Parameters


    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()


    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    #### Sections of local path to most recent date ingestion

    ###### Set of directories based on date
    path_date = year_dir + today_info[:4] + "/" + month_dir + today_info[5:7] + "/"

    ###### Name of file inside directories

    #path_file = path_file_fn(ingest_type)



    #path_file = cont_dat_prefix + today_info + ".pkl"



    ## Requires: download data from API depending on the ingestion type if latest ingestion is outdated
    def requires(self):

        return APIDataIngestion(self.ingest_type)


    ## Run: get most recent local ingestion saved to upload it to s3
    def run(self):

        ###### Name of file inside directories

        path_file = path_file_fn(self.ingest_type)


        ## Location to find most recent local ingestion
        path_full = local_temp_ingestions + self.ingest_type + "/" + self.path_date + path_file


        ## Loading most recent ingestion
        ingesta = pickle.dumps(pickle.load(open(path_full, "rb")))

        ## Storing object in s3
        s3 = get_s3_resource()
        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=ingesta)


    ## Output: uploading data to s3 path
    def output(self):

        client = get_s3_resource_luigi()
        path_file = path_file_fn(self.ingest_type)
        ## Define the path where the ingestion will be stored in s3

        #### Initial part of path
        output_path_start = "s3://{}/{}/{}/".format(
            self.bucket,
            'ingestion',
            self.ingest_type,
        )

        output_path = output_path_start + self.path_date + path_file

        return luigi.contrib.s3.S3Target(output_path, client=client)
