



## Third party imports

import luigi
import luigi.contrib.s3
import pickle
import pandas as pd




## Local application imports
from src.etl.transformation import transform

from src.utils.utils import (
    get_s3_resource,
    get_s3_resource_luigi,
    get_key,
    get_postgres_credentials
)


 ### De aca voy a usar el Today_info
from src.utils.params_gen import (
    #bucket_name,
    #local_temp_ingestions,
    year_dir,
    month_dir,
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

from src.etl.ingesta_almacenamiento import (
    path_file_fn,
    initial_cleaning
)

from src.pipeline.luigi.saves3_metadata import SaveS3Metadata


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

    path_date = year_dir + today_info[:4] + "/" + month_dir + today_info[5:7] + "/"

    ## Requires: download data from API depending on the ingestion type if latest ingestion is outdated
    def requires(self):
        return SaveS3Metadata(ingest_type=self.ingest_type, bucket=self.bucket)

### RUN :

    def run(self):

        ## makes transformation most recent ingestion

        # Agregar lectura de datos desde AWS.

        ## Storing object in s3
        s3 = get_s3_resource()

        ## Geet extraction path:

        path_file = path_file_fn(self.ingest_type)

        ## Define the path where the ingestion will be stored in s3
        extract_path_start = "{}/{}/".format(
            'ingestion',
            self.ingest_type,
        )
        extract_pickle_loc_s3 = extract_path_start + self.path_date + path_file

        ## Reading data form s3

        s3_ingestion = s3.get_object(Bucket=self.bucket, Key=extract_pickle_loc_s3)

        ingestion_pickle_loc_ok = pickle.loads(s3_ingestion['Body'].read())

        ingestion_df = pd.DataFrame(ingestion_pickle_loc_ok)

        transformation = pickle.dumps(transform(ingestion_df, transformation_pickle_loc))

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

        output_path = output_path_start + 'transformation_' + today_info + '.pkl'

        return luigi.contrib.s3.S3Target(output_path, client=client)
