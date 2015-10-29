#
#  Copyright (c) 2015 Intel Corporation 
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

class ModelLinearRegressionTest(unittest.TestCase):
    def testLinearRegression(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/linear_regression_8_columns.csv", schema = [("y", ta.float64),("1",ta.float64),("2",ta.float64),
                                                              ("3",ta.float64),("4",ta.float64),("5",ta.float64),
                                                              ("6",ta.float64),("7",ta.float64),("8",ta.float64),
                                                              ("9",ta.float64),("10",ta.float64)])

        print "create frame"
        frame = ta.Frame(csv,'LinearRegressionSampleFrame')

        print "Initializing a LinearRegressionModel object"
        model = ta.LinearRegressionModel(name='myLinearRegressionModel')

        print "Training the model on the Frame"
        model.train(frame,'y', ['1','2','3','4','5','6','7','8','9','10'])

        output = model.predict(frame)
        self.assertEqual(output.column_names, ['y','1','2','3','4','5','6','7','8','9','10','predicted_value'])


if __name__ == "__main__":
    unittest.main()
