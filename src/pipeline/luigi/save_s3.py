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
        return APIDataIngestion(self)

    def run(self):

        #read file

        ingesta=pickle.load(open('src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl', 'rb'))
        ingesta = pickle.dumps(ingesta)
        #ingesta = pickle.load(infile)

        #read file
        s3 = get_s3_resource()

        print(self.output().path)
        #with self.output().open('w') as output_file:
        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=ingesta)
        #luigi.contrib.s3.S3Client.put(local_path='src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl',destination_s3_path=self.output().path)

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

        #return luigi.contrib.s3.S3Client()
