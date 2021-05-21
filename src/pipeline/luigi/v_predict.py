import luigi
import luigi.contrib.s3
import pickle

from src.utils.utils import (
    get_s3_resource,
    get_s3_resource_luigi,
    get_key,
)

from src.utils.params_gen import (
    today_info,
    ms_aws_key
)

from src.pipeline.luigi.u_bias_fairness_metadata import BiasFairnessMetadata



class Predict(luigi.Task):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()

    ## Requires: assessing that model training metadata is stored
    def requires(self):
        return BiasFairnessMetadata(ingest_type=self.ingest_type, bucket=self.bucket)


    ## Run: selecting best trained model
    def run(self):

        ## Establishing connection with S3
        s3 = get_s3_resource()

        ## Loading latest model
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=ms_aws_key)['Contents']
        obj_path = [file["Key"] for file in objects][-1]
        response = s3.get_object(
            Bucket=bucket,
            Key=obj_path
        )
        #### Latest model stored in S3
        sel_model = pickle.loads(response["Body"].read())["best_trained_model"]

        mt_pickle_loc_s3 = 'trained_models/trained_models_' + today_info + '.pkl'

        mt_results_pkl = s3.get_object(Bucket=self.bucket, Key=mt_pickle_loc_s3)

        mt_results_dict = pickle.loads(mt_results_pkl['Body'].read())

        ms_results_dict = model_selection(mt_results_dict, ms_results_pickle_loc)

        ms_results_pkl = pickle.dumps(ms_results_dict)

        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=ms_results_pkl)


    ## Output: uploading data to s3 path
    def output(self):

        ## Connecting to AWS using luigi
        client = get_s3_resource_luigi()

        ## Define the path where the ingestion will be stored in s3
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            'predictions',
        )

        output_path = output_path_start + 'predictions_' + today_info + '.pkl'

        return luigi.contrib.s3.S3Target(output_path, client=client)
