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
import trustedanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class ModelRandomForestTest(unittest.TestCase):
    def testSvm(self):
        print "define csv file"
        csv = ia.CsvFile("/datasets/RandomForest.csv", schema= [('Class', int),
                                                               ('Dim_1', ia.float64),
                                                               ('Dim_2',ia.float64)])

        print "create frame"
        frame = ia.Frame(csv)

        print "Initializing the classifier model object"
        classifier = ia.RandomForestClassifierModel()

        print "Training the model on the Frame"
        classifier.train(frame,'Class', ['Dim_1','Dim_2'],num_classes=2)

        print "Predicting on the Frame"
        output = classifier.predict(frame)

        self.assertEqual(output.column_names, ['Class', 'Dim_1','Dim_2', 'predicted_class'])

        print "Initializing the classifier model object"
        regressor = ia.RandomForestRegressorModel()

        print "Training the model on the Frame"
        regressor.train(frame,'Class', ['Dim_1','Dim_2'])

        print "Predicting on the Frame"
        output = regressor.predict(frame)

        self.assertEqual(output.column_names, ['Class', 'Dim_1','Dim_2', 'predicted_class'])



if __name__ == "__main__":
    unittest.main()
