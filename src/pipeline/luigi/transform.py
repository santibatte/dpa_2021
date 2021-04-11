



## Third party imports

import luigi
import luigi.contrib.s3
import pickle



## Local application imports
from src.etl.transformation import transform

from src.utils.utils import (
    get_s3_resource,
    get_s3_resource_luigi,
    get_key,
)


 ### De aca voy a usar el Today_info
from src.utils.params_gen import (
    #bucket_name,
    #local_temp_ingestions,
    #year_dir,
    #month_dir,
    #hist_ingest_path,
    #hist_dat_prefix,
    #cont_ingest_path,
    #cont_dat_prefix,
    today_info
)

from src.utils.params_gen import (
    ingestion_pickle_loc,
    transformation_pickle_loc,
    #cat_reduction_ref,
    #regex_violations,
    #serious_viols,
)

from src.pipeline.luigi.save_s3 import S3Task


"----------------------------------------------------------------------------------------------------------------------"
################
## Luigi task ##
################


## Task aimed to store transformation in s3
class Transformation(luigi.Task):


    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()

    ## Requires: download data from API depending on the ingestion type if latest ingestion is outdated
    def requires(self):

        return S3Task(ingest_type=self.ingest_type, bucket=self.bucket)




### RUN :

    def run(self):

        ## makes transformation most recent ingestion

        # Agregar lectura de datos desde AWS.

        transformation = pickle.dumps(transform(ingestion_pickle_loc, transformation_pickle_loc))

        ## Storing object in s3
        s3 = get_s3_resource()
        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=transformation)




### OUTPUT:


    ## Output: uploading data to s3 path
    def output(self):

        ## Conecting to AWS using luigi
        client = get_s3_resource_luigi()

        ## Define the path where the ingestion will be stored in s3
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            'transformation',
        )

        output_path = output_path_start + 'transformation_' +  today_info +'.pkl'

        return luigi.contrib.s3.S3Target(output_path, client=client)