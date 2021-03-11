import luigi
import luigi.contrib.s3
import boto3
import os

from src.pipeline.luigi.extract_from_api import APIDataIngestion
from src.etl.ingesta_almacenamiento import get_s3_resource

class S3Task(luigi.Task):

    ingest_type = luigi.Parameter()

    def requires(self):
        return APIDataIngestion(self)

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    etl_path = luigi.Parameter()
    year = luigi.Parameter()
    month = luigi.Parameter()



    def run(self):

        ## read file...

        s3 = get_s3_resource()
        #ses = boto3.session.Session(profile_name='mge', region_name='us-west-2')
        #s3_resource = ses.resource('s3')

        #with self.output().open('w') as output_file:
        #    output_file.write("test,luigi,s3")


    def output(self):
        output_path = "s3://{}/{}/{}/{}/YEAR={}/MONTH={}/ingesta.pkl".\
        format(self.bucket,
        self.root_path,
        self.etl_path,
        self.task_name,
        self.year,
        str(self.month))

        return luigi.contrib.s3.S3Target(path=output_path)
