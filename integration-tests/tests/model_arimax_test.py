# vim: set encoding=utf-8

#
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

if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class ModelArimaxTest(unittest.TestCase):
    def test_arimax_air_quality(self):
        # Data from Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml]. Irvine, CA: University of California, School of Information and Computer Science.
        print "Define csv file"
        schema = [("Date", str), ("Time", str), ("CO_GT", ta.float64), ("PT08_S1_CO", ta.float64), ("NMHC_GT", ta.float64), ("C6H6_GT", ta.float64), ("PT08_S2_NMHC", ta.float64), ("NOx_GT", ta.float64),
                  ("PT08_S3_NOx", ta.float64), ("NO2_GT", ta.float64), ("PT08_S4_NO2", ta.float64), ("PT08_S5_O3", ta.float64), ("T", ta.float64), ("RH", ta.float64), ("AH", ta.float64)]
        csv = ta.CsvFile("/datasets/arimax_train.csv", schema=schema, skip_header_lines=1)

        print "Create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArimaxModel object"
        arimax = ta.ArimaxModel()

        print "Training the model on the Frame"
        arimax.train(train_frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"], 1, 1, 1, 1, True, False)

        print "Create test frame"
        csv2 = ta.CsvFile("/datasets/arimax_test.csv", schema=schema, skip_header_lines=1)
        test_frame = ta.Frame(csv2)

        print "Predicting on the Frame"
        p = arimax.predict(test_frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"])

        expected_results = [[3.9, 3.1384052036036163],
                            [3.7, 2.2096085801345],
                            [6.6, 3.052618296503863],
                            [4.4, 2.1495532900204375],
                            [3.5, 2.929771168550256],
                            [5.4, 2.155756454454324],
                            [2.7, 2.8784218519015745],
                            [1.9, 2.1528352219380147],
                            [1.6, 2.7830795782099473],
                            [1.7, 2.1096269282113664],
                            [-200.0, 2.8628707912495215],
                            [1.0, 2.0471200633069278],
                            [1.2, 2.7726186606363887],
                            [1.5, 2.0820391788568395],
                            [2.7, 2.9878888229516978],
                            [3.7, 2.3182512709816443],
                            [3.2, 3.211283519783637],
                            [4.1, 2.5541133101407363],
                            [3.6, 3.268861636132588],
                            [2.8, 2.467897319671856]]

        self.assertEqual(expected_results, p.take(20, columns=["CO_GT", "predicted_y"]))

if __name__ == "__main__":
    unittest.main()
