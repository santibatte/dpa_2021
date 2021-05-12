## Third party imports

import luigi
import luigi.contrib.s3
import pickle
import pandas as pd

## Testing imports
import unittest
import marbles.core
from io import StringIO
from datetime import (date, datetime)

## Local application imports

from src.pipeline.luigi.r_model_selection_metadata import ModelSelectionMetadata



from src.utils.utils import (
    get_s3_resource,
    get_s3_resource_luigi,
    get_key,
    write_csv_from_df,
)

from src.pipeline.bias_fairness import (bias_fairness)
from src.utils.params_gen import (
    metadata_dir_loc,
    tests_dir_loc,
    today_info,
    aq_results_pickle_loc,
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

        ## Establishing connection with S3
        s3 = get_s3_resource()


        ## Building aequitas dataframe based on previous models results

        #### (Models training results) - building initial df with unique IDs and real test labels
        mt_results_s3_pth = 'trained_models/trained_models_' + today_info + '.pkl'
        mt_results_pkl = s3.get_object(Bucket=self.bucket, Key=mt_results_s3_pth)
        mt_results = pickle.loads(mt_results_pkl['Body'].read())
        df_aeq = mt_results["test_labels"].to_frame()

        #### (Model selection results) - adding labels predicted by best model
        ms_results_s3_pth = 'model_selection/selected_model_' + today_info + '.pkl'
        ms_results_pkl = s3.get_object(Bucket=self.bucket, Key=ms_results_s3_pth)
        ms_results = pickle.loads(ms_results_pkl['Body'].read())
        df_aeq["score"] = ms_results["model_test_predict_labels"]

        #### (Transformation results) - adding zip and reference group
        tr_results_s3_pth = 'transformation/transformation_' + today_info + '.pkl'
        tr_results_pkl = s3.get_object(Bucket=self.bucket, Key=tr_results_s3_pth)
        tr_results = pickle.loads(tr_results_pkl['Body'].read())
        df_aeq = df_aeq.join(tr_results.loc[:, ["zip-income-class"]], how="inner")

        #### Renaming columns for aequitas analysis
        df_aeq.rename(columns={
            "label": "label_value",
            "model_test_predict_labels": "score",
            "zip-income-class": "reference_group"
        }, inplace=True)

        #### Converting index "inspection-id" to column
        df_aeq.reset_index(inplace=True, drop=True)

        print("***********")
        print(df_aeq.columns)
        print("***********")


        ## Running unit test
        class TestBiasFairness(marbles.core.TestCase):
            def test_df_aeq(self):
                columns_names = list(df_aeq.columns)
                df_expected_names=['label_value', 'score', 'reference_group']
                print("")
                print("***********")
                print(len(columns_names))
                print(len(columns_names))
                print(df_aeq.shape[1])
                print(len(df_expected_names))
                print("***********")
                self.assertEqual(df_aeq.shape[1], 3, note='Oops, columns are missing!')

        stream = StringIO()
        runner = unittest.TextTestRunner(stream=stream)
        result = runner.run(unittest.makeSuite(TestBiasFairness))

        suite = unittest.TestLoader().loadTestsFromTestCase(TestBiasFairness)

        with open(tests_dir_loc + 'test_bias_fairness.txt', 'w') as f:
            unittest.TextTestRunner(stream=f, verbosity=2).run(suite)

        res = []
        with open(tests_dir_loc + "test_bias_fairness.txt") as fp:
            lines = fp.readlines()
            for line in lines:
                if "FAILED" in line:
                    res.append([str(datetime.now()), "FAILED, Columns are missing."])
                if "OK" in line:
                    res.append([str(datetime.now()), "PASS"])

        res_df = pd.DataFrame(res, columns=['Date', 'Result'])

        res_df.to_csv(tests_dir_loc + 'bias_fairness_unittest.csv', index=False)




        ## Do Bias_fairness and save results:

        aeq_results_dict=bias_fairness(df_aeq)

        pickle.dump(aeq_results_dict, open(aq_results_pickle_loc, "wb"))
        aq_pickle = pickle.dumps(aeq_results_dict)

        s3.put_object(Bucket=self.bucket, Key=get_key(self.output().path), Body=aq_pickle)



    ## Output: uploading data to s3 path
    def output(self):

        ## Connecting to AWS using luigi
        client = get_s3_resource_luigi()

        ## Define the path where the ingestion will be stored in s3
        output_path_start = "s3://{}/{}/".format(
            self.bucket,
            'bias_fairness',
        )

        output_path = output_path_start + 'bias_fairness_' +  today_info +'.pkl'

        return luigi.contrib.s3.S3Target(output_path, client=client)
