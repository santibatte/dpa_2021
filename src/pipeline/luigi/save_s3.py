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
)

from src.utils.params_gen import (
    local_temp_ingestions,
    year_dir,
    month_dir,
    cont_dat_prefix,
    today_info,
)





"----------------------------------------------------------------------------------------------------------------------"
################
## Luigi task ##
################


def get_key(s3_name):
    split = s3_name.split(sep='/')[3:]
    join = '/'.join(split)
    return join


## Task aimed to store ingestion in s3
class S3Task(luigi.Task):


    ## Parameters

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()

    #### Local path to most recent date ingestion

    ###### Initial path to local ingestions
    path_to_ing_type = local_temp_ingestions + str(ingest_type)

    ###### Set of directories based on date
    path_date = year_dir + today_info[:4] + "/" + month_dir + today_info[5:7] + "/"

    ###### Name of file inside directories
    path_file = cont_dat_prefix + today_info + ".pkl"

    ###### Concatenating all parts
    path_full = path_to_ing_type + path_date + path_file


    ## Requires: download data from API depending on the ingestion type if latest ingestion is outdated
    def requires(self):

        return APIDataIngestion(self.ingest_type)


    ## Run: get most recent local ingestion saved to upload it to s3
    def run(self):


        ## Loading most recent ingestion
        ingesta = pickle.dumps(pickle.load(open(self.path_full, "rb")))

        ## Storing object in s3
        s3 = get_s3_resource()
        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=ingesta)


    ## Output: uploading data to s3 path
    def output(self):

        client = get_s3_resource_luigi()

        ## Define the path where the ingestion will be stored in s3

        #### Initial part of path
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            self.ingest_type,
        )

        output_path = output_path_start + self.path_date + self.path_file

        return luigi.contrib.s3.S3Target(output_path, client=client)

