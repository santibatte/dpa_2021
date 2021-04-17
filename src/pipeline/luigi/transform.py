



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
    path_file_fn
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

        #path_file = path_file_fn(self.ingest_type)

        ## Define the path where the ingestion will be stored in s3
        #extract_path_start = "{}/{}/".format(
        #    'ingestion',
        #    self.ingest_type,
        #)
        #extract_pickle_loc_s3 = extract_path_start + self.path_date + path_file

        #Reads from local file not from S3
        ingestion_pickle_loc_ok = ingestion_pickle_loc

        #s3_ingestion = s3.get_object(Bucket=self.bucket,Key=extract_pickle_loc_s3)
        #df = pickle.load(open(s3_ingestion['Body'].read(), 'rb'))
        #df = pickle.loads(open(s3_ingestion['Body'].read()))

        #hola= s3_ingestion['Body'].read()

        #print(type(hola))  ## <class 'bytes'>


        #print ('esto es', hola)  ##  b'\x80\x03C\x06\x80\x03]q\x00.q\x00.'

        #df = pickle.loads(open(s3_ingestion['Body'].read(), 'rb')) ##esto da error.... no such file b.

        #df = pickle.loads(hola)

        #print( 'miau aca', type(df))  #  <class 'bytes'>

        #print(df)  ### es esto, no es un DF > b'\x80\x03]q\x00.'


        #unpickled_df = pd.read_pickle(df)



#pickle.load(open(path, "rb"))
        #print('el pickle es', type(unpickled_df))

        ## transformation_luigi es un data frame?

## Hay que modificar el codigo para que tome los datos de s3:
        # MAke Data Transformation
        #transformation = pickle.dumps(transform(df, transformation_pickle_loc))
        transformation = pickle.dumps(transform(ingestion_pickle_loc_ok, transformation_pickle_loc))








        #Stores transformation in S3

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
