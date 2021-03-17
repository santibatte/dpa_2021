## General imports
import luigi
import luigi.contrib.s3
import boto3
import os
import pickle

## Local imports
from src.pipeline.luigi.extract import APIDataIngestion
from src.etl.ingesta_almacenamiento import get_s3_resource


def get_key(s3_name):
    split=s3_name.split(sep='/')[3:]
    join= '/'.join(split)
    return join


class S3Task(luigi.Task):

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    ingest_type = luigi.Parameter() #initial
    year = luigi.Parameter()
    month = luigi.Parameter()

    def requires(self):
        return APIDataIngestion(self.ingest_type)

    def run(self):

        #read file
        #if ingest_type.self == 'initial': ()
        ingesta=pickle.load(open('src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl', 'rb')) ## cambiar nombre buscar el archivo initiarl.
        ingesta = pickle.dumps(ingesta)

        #elif ingest_type.self == 'consecutive':
            #ingesta=pickle.load(open('src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl', 'rb')) ## cambiar nombre, mirar el archivo llamado CONSECUTIVE.
            #ingesta = pickle.dumps(ingesta)



        s3 = get_s3_resource()
        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=ingesta)


    def output(self):
        #output_path = "{}/{}/YEAR={}/MONTH={}/ingesta.pkl".\
        output_path = "s3://{}/{}/{}/YEAR={}/MONTH={}/ingesta.pkl".\
        format(self.bucket,
        self.root_path,
        self.ingest_type,
        #self.task_name,
        self.year,
        str(self.month))

        return luigi.contrib.s3.S3Target(output_path)
