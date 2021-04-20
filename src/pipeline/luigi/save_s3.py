## MODULE WITH TASK TO SAVE EXTRACTIONS IN S3





"----------------------------------------------------------------------------------------------------------------------"
#############
## Imports ##
#############


## Standard library imports

import os

import pickle

from datetime import (date, datetime)

import pandas as pd


## Third party imports

import luigi
import luigi.contrib.s3

import boto3


## Local application imports

from src.pipeline.luigi.extract_metadata import ExtractMetadata

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

    ## Metadata
    metadata_dir_loc,
    save_s3_metadata,
    save_s3_metadata_index,
    save_s3_metadata_csv_name,
)

from src.etl.ingesta_almacenamiento import (
    path_file_fn
)



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



    ## Requires: download data from API depending on the ingestion type if latest ingestion is outdated
    #def requires(self):
#        return APIDataIngestion(self.ingest_type)

    def requires(self):
        return ExtractMetadata(self.ingest_type)


    ## Run: get most recent local ingestion saved to upload it to s3
    def run(self):

        ###### Name of file inside directories

        path_file = path_file_fn(self.ingest_type)


        ## Location to find most recent local ingestion
        path_full = local_temp_ingestions + self.ingest_type + "/" + self.path_date + path_file


        ## Loading most recent ingestion
        ingesta_df = pickle.load(open(path_full, "rb"))


        ## Obtaining task metadata
        
        #### Storing time execution metadata
        save_s3_metadata[save_s3_metadata_index] = str(datetime.now())
        #### Bucket where data will be saved
        save_s3_metadata["s3_bucket_name"] = str(self.bucket)
        #### S3 key related to the data
        save_s3_metadata["s3_key_name"] = str(get_key(self.output().path))
        #### Shape of df going into s3
        save_s3_metadata["df_shape"] = str(ingesta_df.shape)

        #### Converting dict to df and writing contents to df
        df_meta = pd.DataFrame.from_dict(save_s3_metadata, orient="index").T
        df_meta.set_index(save_s3_metadata_index, inplace=True)
        write_csv_from_df(df_meta, metadata_dir_loc, save_s3_metadata_csv_name)


        ## Storing object in s3 as pickle
        ingesta_pkl = pickle.dumps(ingesta_df)
        s3 = get_s3_resource()
        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=ingesta_pkl)


    ## Output: uploading data to s3 path
    def output(self):

        ## Conecting to AWS using luigi
        client = get_s3_resource_luigi()
        path_file = path_file_fn(self.ingest_type)


        ## Define the path where the ingestion will be stored in s3
        output_path_start = "s3://{}/{}/{}/".format(
            self.bucket,
            'ingestion',
            self.ingest_type,
        )
        output_path = output_path_start + self.path_date + path_file

        return luigi.contrib.s3.S3Target(output_path, client=client)
