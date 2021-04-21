#Completar con un cargar de S3 y guardar en S3

import luigi
import luigi.contrib.s3
import pickle


from src.pipeline.luigi.l_feature_engineering_metadata import FeatureEngineeringMetadata

class ModelTraining(luigi.Task):

    def requires(self):
        return FeatureEngineeringMetadata(ingest_type=self.ingest_type, bucket=self.bucket)


    def run(self):

        s3 = get_s3_resource()

        feature_engineering_pickle_loc_s3 = 'feature_engineering/feature_engineering_' + today_info + '.pkl'

        model_training_luigi = s3.get_object(Bucket=self.bucket, Key=feature_engineering_luigi)

        df_pre_training = pickle.loads(model_training_luigi['Body'].read())

        model_training = aqui_entrenamos_modelo  con parametro : df_pre_transformation

        model_training_pkl = pickle.dumps(model_training)

        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=model_training_pkl)


    ## Output: uploading data to s3 path
    def output(self):

        ## Conecting to AWS using luigi
        client = get_s3_resource_luigi()

        ## Define the path where the ingestion will be stored in s3
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            'trained_models',
        )

        output_path = output_path_start + 'trained_models_' +  today_info +'.pkl'

        return luigi.contrib.s3.S3Target(output_path, client=client)
