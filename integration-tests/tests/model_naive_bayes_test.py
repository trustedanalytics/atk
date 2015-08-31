#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import unittest
import trustedanalytics as atk

# show full stack traces
atk.errors.show_details = True
atk.loggers.set_api()
# TODO: port setup should move to a super class
if atk.server.port != 19099:
    atk.server.port = 19099
atk.connect()

class ModelNaiveBayesTest(unittest.TestCase):
    def test_naive_bayes(self):
        print "define csv file"
        schema = [("Class", atk.int32),("Dim_1", atk.int32),("Dim_2", atk.int32),("Dim_3",atk.int32)]
        train_file = atk.CsvFile("/datasets/naivebayes_spark_data.csv", schema= schema)
        print "creating the frame"
        train_frame = atk.Frame(train_file)

        print "initializing the naivebayes model"
        n = atk.NaiveBayesModel()

        print "training the model on the frame"
        n.train(train_frame, 'Class', ['Dim_1', 'Dim_2', 'Dim_3'])

        print "predicting the class using the model and the frame"
        output = n.predict(train_frame)
        self.assertEqual(output.column_names, ['Class', 'Dim_1', 'Dim_2', 'Dim_3','predicted_class'])


if __name__ == "__main__":
    unittest.main()
