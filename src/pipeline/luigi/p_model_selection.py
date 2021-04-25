
import luigi
import luigi.contrib.s3
import pickle

from src.utils.utils import (
    get_s3_resource,
    get_s3_resource_luigi,
    get_key,
    write_csv_from_df,
)


class ModelSelection(luigi.Task):

    #### Bucket where all ingestions will be stored in AWS S3
    bucket = luigi.Parameter()

    #### Defining the ingestion type to Luigi (`consecutive` or `initial`)
    ingest_type = luigi.Parameter()


    def requires(self):
        return FeatureEngineeringMetadata(ingest_type=self.ingest_type, bucket=self.bucket)


    def run(self):

        s3 = get_s3_resource()

        trained_model_pickle_loc_s3 = 'trained_models/trained_models_' + today_info + '.pkl'

        model_selection_luigi = s3.get_object(Bucket=self.bucket, Key=trained_model_pickle_loc_s3)

        df_pre_model_selection = pickle.loads(model_selection_luigi['Body'].read())

        model_selection =   df_pre_model_selection ###  aqui_entrenamos_modelo  con parametro : df_pre_model_selection

        model_selection_pkl = pickle.dumps(model_selection)

        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=model_selection_pkl)


    ## Output: uploading data to s3 path
    def output(self):

        ## Conecting to AWS using luigi
        client = get_s3_resource_luigi()

        ## Define the path where the ingestion will be stored in s3
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            'model_selection',
        )

        output_path = output_path_start + 'trained_model_' +  today_info +'.pkl'

        return luigi.contrib.s3.S3Target(output_path, client=client)
