# vim: set encoding=utf-8

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

class ModelArxTest(unittest.TestCase):
    def testSvm(self):
        print "define csv file"
        schema = [("y", ta.float64),("visitors", ta.float64),("wkends", ta.float64),("seasonality", ta.float64),("incidentRate", ta.float64), ("holidayFlag", ta.float64),("postHolidayFlag", ta.float64),("mintemp", ta.float64)]
        csv = ta.CsvFile("/datasets/arx_train.csv", schema=schema, skip_header_lines=1)

        print "create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArxModel object"
        arx = ta.ArxModel()

        print "Training the model on the Frame"
        arx.train(train_frame, "y", ["visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp"],0,0,True)

        print "create test frame"
        csv = ta.CsvFile("/datasets/arx_test.csv", schema=schema, skip_header_lines=1)
        test_frame = ta.Frame(csv)

        print "Predicting on the Frame"
        p = arx.predict(test_frame, "y", ["visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp"])
        self.assertEqual(p.column_names, ["y","visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp","predicted_y"])

        expected_results = [[99.99999234330198],
                            [98.00000220169095],
                            [101.99999803760333],
                            [98.00000071010813],
                            [111.99999886664024],
                            [99.00000373787175],
                            [99.00000353440495],
                            [86.99999823659364],
                            [103.00000236184275],
                            [114.99999178843603],
                            [100.9999939917012],
                            [124.99999319338036],
                            [116.9999989603231],
                            [109.00000481908955],
                            [110.99999666776476],
                            [104.99999266331749]]

        self.assertEqual(expected_results, p.take(p.row_count, 0, "predicted_y"))


if __name__ == "__main__":
    unittest.main()
