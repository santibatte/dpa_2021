## Third party imports

import luigi
import luigi.contrib.s3
import pickle


## Local application imports

from src.pipeline.luigi.r_model_selection_metadata import ModelSelectionMetadata



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


class BiasFairness(luigi.Task):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()

    ## Requires: download data from API depending on the ingestion type if latest ingestion is outdated
    def requires(self):
        return ModelSelectionMetadata(ingest_type=self.ingest_type, bucket=self.bucket)


    def run(self):


        s3 = get_s3_resource()

        #Load Best Model :
        model_selection_pickle_loc_s3 = 'model_selection/model_selection_' + today_info + '.pkl'

        model_selection_luigi = s3.get_object(Bucket=self.bucket, Key=model_selection_pickle_loc_s3)

        #Load best model we need for predict proba
        best_model = pickle.loads(model_selection_luigi['Body'].read())

        ## Load S3 FE data frame in order to do bias fairness
        fe_pickle_loc_s3 = 'feature_engineering/feature_engineering_' + today_info + '.pkl'

        fe_results_pkl = s3.get_object(Bucket=self.bucket, Key=fe_pickle_loc_s3)

        fe_results_dict = pickle.loads(fe_results_pkl['Body'].read())


        ## Do Bias_fairness and save results:

        bias_fairness =  best_model # We should delete "best_model" and call bias_fairness main function here

        bias_fairness_pickle = pickle.dumps(bias_fairness)


        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=bias_fairness_pickle)


    ## Output: uploading data to s3 path
    def output(self):

        ## Conecting to AWS using luigi
        client = get_s3_resource_luigi()

        ## Define the path where the ingestion will be stored in s3
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            'bias_fairness',
        )

        output_path = output_path_start + 'bias_fairness_' +  today_info +'.pkl'

        return luigi.contrib.s3.S3Target(output_path, client=client)
