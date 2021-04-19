
## Third party imports

import luigi
import luigi.contrib.s3
import pickle


## Local application imports

from src.pipeline.luigi.transform_metadata import TransformationMetadata

from src.pipeline.feature_engineering import feature_engineering
from src.utils.params_gen import (
    transformation_pickle_loc,
    fe_pickle_loc_imp_features,
    fe_pickle_loc_feature_labs,
)

from src.utils.utils import (
    get_s3_resource,
    get_s3_resource_luigi,
    get_key,
    write_csv_from_df,
)


from src.utils.params_gen import (
    metadata_dir_loc,

    transformation_pickle_loc,
    fe_pickle_loc_imp_features,
    fe_pickle_loc_feature_labs,
    today_info,

    fe_metadata_csv_name,
)


class FeatureEngineering(luigi.Task):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()

    ## Requires: download data from API depending on the ingestion type if latest ingestion is outdated
    def requires(self):
        return TransformationMetadata(ingest_type=self.ingest_type, bucket=self.bucket)


    def run(self):

        ## makes feature engeneering from transformed data

        #Reads from local computer
        # feature_engineering_luigi = pickle.dumps(feature_engineering(transformation_pickle_loc,\
        #fe_pickle_loc_imp_features, fe_pickle_loc_feature_labs)
        ## Storing object in s3
        s3 = get_s3_resource()

        ##  s3.get(s3_path, destination_local_path)
        ## Read from S3 instead of local computer
        transformation_pickle_loc_s3 = transformation/'transformation_' +  today_info +'.pkl'

        feature_engineering_luigi = s3.get_object(Bucket=self.bucket, Key =  transformation_pickle_loc_s3)

        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=feature_engineering_luigi)


    ## Output: uploading data to s3 path
    def output(self):

        ## Conecting to AWS using luigi
        client = get_s3_resource_luigi()

        ## Define the path where the ingestion will be stored in s3
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            'feature_engineering',
        )

        output_path = output_path_start + 'feature_engineering_' +  today_info +'.pkl'

        return luigi.contrib.s3.S3Target(output_path, client=client)
