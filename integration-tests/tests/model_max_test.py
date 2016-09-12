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

    def test_max_air_quality(self):
        # Data from Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml]. Irvine, CA: University of California, School of Information and Computer Science.
        print "Define csv file"
        schema = [("Date", str), ("Time", str), ("CO_GT", ta.float64), ("PT08_S1_CO", ta.float64), ("NMHC_GT", ta.float64), ("C6H6_GT", ta.float64), ("PT08_S2_NMHC", ta.float64), ("NOx_GT", ta.float64),
                  ("PT08_S3_NOx", ta.float64), ("NO2_GT", ta.float64), ("PT08_S4_NO2", ta.float64), ("PT08_S5_O3", ta.float64), ("T", ta.float64), ("RH", ta.float64), ("AH", ta.float64)]
        csv = ta.CsvFile("/datasets/arimax_train.csv", schema=schema, skip_header_lines=1)

        print "Create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArimaxModel object"
        max = ta.MaxModel()

        print "Training the model on the Frame"
        max.train(train_frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"], 3, 0, True, False)

        print "Create test frame"
        csv2 = ta.CsvFile("/datasets/arimax_test.csv", schema=schema, skip_header_lines=1)
        test_frame = ta.Frame(csv2)

        print "Predicting on the Frame"
        p = max.predict(test_frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"])

        expected_results = [[3.9, 0.40372259585936543],
                            [3.7, 6.6634901462882725],
                            [6.6, 5.981442062684975],
                            [4.4, 5.35837518115529],
                            [3.5, 5.026072844339458],
                            [5.4, 4.569157131217689],
                            [2.7, 4.029165833891962],
                            [1.9, 3.9460902496880044],
                            [1.6, 3.779939081280088],
                            [1.7, 3.655325704974152],
                            [-200.0, 3.2399477839543613],
                            [1.0, 2.9076454471385293],
                            [1.2, 3.4476367444642566],
                            [1.5, 2.9907210313424875],
                            [2.7, 2.6168809024246764],
                            [3.7, 2.6999564866286345],
                            [3.2, 3.987628041789983],
                            [4.1, 5.150686220645396],
                            [3.6, 6.479895567908723],
                            [2.8, 7.642953746764134]]

        self.assertEqual(expected_results, p.take(20, columns=["CO_GT", "predicted_y"]))


if __name__ == "__main__":
    unittest.main()
