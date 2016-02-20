# vim: set encoding=utf-8
#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


import unittest
import trustedanalytics as ta

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class ModelSmokeTest(unittest.TestCase):
    """
    Smoke test basic frame operations to verify functionality that will be needed by all other tests.

    If these tests don't pass, there is no point in running other tests.

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    def test_model(self):
        print "Initialize KMeansModel object"
        k = ta.KMeansModel()

        print "Initialize LogisticRegressionModel object"
        l = ta.LogisticRegressionModel()

        print "Initialize NaiveBayesModel object"
        n = ta.NaiveBayesModel()

    def test_kmeans_train_publish(self):

        frame = ta.Frame(ta.UploadRows([[2,"ab"],[1,"cd"],[7,"ef"],[1,"gh"],[9,"ij"],[2,"kl"],[0,"mn"],[6,"op"],[5,"qr"]],
                                       [("data", ta.float64),("name", str)]))
        model = ta.KMeansModel()
        train_output = model.train(frame, ["data"], [1], 3)
        self.assertTrue(train_output.has_key('within_set_sum_of_squared_error'))
        model.publish()

if __name__ == "__main__":
    unittest.main()
