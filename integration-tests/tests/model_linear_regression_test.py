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

class ModelLinearRegressionTest(unittest.TestCase):
    def testLinearRegression(self):
        print "define csv file"
        csv = atk.CsvFile("/datasets/linear_regression_8_columns.csv", schema = [("y", atk.float64),("1",atk.float64),("2",atk.float64),
                                                              ("3",atk.float64),("4",atk.float64),("5",atk.float64),
                                                              ("6",atk.float64),("7",atk.float64),("8",atk.float64),
                                                              ("9",atk.float64),("10",atk.float64)])

        print "create frame"
        frame = atk.Frame(csv,'LinearRegressionSampleFrame')

        print "Initializing a LinearRegressionModel object"
        model = atk.LinearRegressionModel(name='myLinearRegressionModel')

        print "Training the model on the Frame"
        model.train(frame,'y', ['1','2','3','4','5','6','7','8','9','10'])

        output = model.predict(frame)
        self.assertEqual(output.column_names, ['y','1','2','3','4','5','6','7','8','9','10','predicted_value'])


if __name__ == "__main__":
    unittest.main()
