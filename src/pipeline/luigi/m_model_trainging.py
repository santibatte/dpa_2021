#Completar con un cargar de S3 y guardar en S3

import luigi
import luigi.contrib.s3
import pickle


from src.pipeline.luigi.l_feature_engineering_metadata import FeatureEngineeringMetadata

from src.utils.params_gen import (
    mt_results_pickle_loc,
    today_info,
)





class ModelTraining(luigi.Task):


    ## Requires: assessing that feature engineering metadata is stored
    def requires(self):
        return FeatureEngineeringMetadata(ingest_type=self.ingest_type, bucket=self.bucket)


    ## Run: training models with engineered features
    def run(self):

        s3 = get_s3_resource()

        fe_pickle_loc_s3 = 'feature_engineering/feature_engineering_' + today_info + '.pkl'

        fe_results_pkl = s3.get_object(Bucket=self.bucket, Key=fe_pickle_loc_s3)

        fe_results_dict = pickle.loads(fe_results_pkl['Body'].read())

        mt_results_dict = models_training(fe_results_dict, mt_results_pickle_loc)

        mt_pkl = pickle.dumps(mt_results_dict)

        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=mt_pkl)


    ## Output: uploading data to s3 path
    def output(self):

        ## Connecting to AWS using luigi
        client = get_s3_resource_luigi()

        ## Define the path where the ingestion will be stored in s3
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            'trained_models',
        )

        output_path = output_path_start + 'trained_models_' + today_info + '.pkl'

        return luigi.contrib.s3.S3Target(output_path, client=client)
