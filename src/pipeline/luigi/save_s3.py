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

from src.etl.ingesta_almacenamiento import get_s3_resource





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

    #### AWS S3 parameters
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    ## Requires: download data from API depending on the ingestion type if latest ingestion is outdated
    def requires(self):

        return APIDataIngestion(self.ingest_type)


    ## Run: get most recent local ingestion saved to upload it to s3
    def run(self):

        #read file
        #if ingest_type.self == 'initial': ()
        ingesta=pickle.load(open('src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl', 'rb')) ## cambiar nombre buscar el archivo initiarl.
        ingesta = pickle.dumps(ingesta)

        #elif ingest_type.self == 'consecutive':
            #ingesta=pickle.load(open('src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl', 'rb')) ## cambiar nombre, mirar el archivo llamado CONSECUTIVE.
            #ingesta = pickle.dumps(ingesta)



        s3 = get_s3_resource()
        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=ingesta)


    ## Output: uploading data to s3 path
    def output(self):

        output_path = "s3://{}/{}/{}/YEAR={}/MONTH={}/ingesta.pkl".format(
            self.bucket,
            self.root_path,
            self.ingest_type,
            #self.task_name,
            self.year,
            str(self.month)
        )

        return luigi.contrib.s3.S3Target(output_path)
