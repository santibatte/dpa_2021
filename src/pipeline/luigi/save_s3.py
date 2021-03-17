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

from src.utils.utils import get_s3_resource

from src.utils.params_gen import (
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


    ## Requires: download data from API depending on the ingestion type if latest ingestion is outdated
    def requires(self):

        return APIDataIngestion(self.ingest_type)


    ## Run: get most recent local ingestion saved to upload it to s3
    def run(self):

        #read file
        #if ingest_type.self == 'initial': ()
        ingesta = pickle.load(open('src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl', 'rb')) ## cambiar nombre buscar el archivo initiarl.
        ingesta = pickle.dumps(ingesta)

        #elif ingest_type.self == 'consecutive':
            #ingesta=pickle.load(open('src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl', 'rb')) ## cambiar nombre, mirar el archivo llamado CONSECUTIVE.
            #ingesta = pickle.dumps(ingesta)



        s3 = get_s3_resource()
        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=ingesta)


    ## Output: uploading data to s3 path
    def output(self):

        ## Define the path where the ingestion will be stored in s3

        #### Initial part of path
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            self.ingest_type,
        )

        #### Set of directories based on date
        out_path_date = year_dir + today_info[:4] + month_dir + today_info[5:7]

        #### Name of file inside directories
        out_path_file = cont_dat_prefix + today_info + ".pkl"

        #### Concatenating all parts
        output_path = output_path_start + out_path_date + out_path_file


        return luigi.contrib.s3.S3Target(output_path)

