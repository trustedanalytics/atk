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
    def test_arimax_with_lag(self):
        print "define csv file"
        schema = [("y", ta.float64),("visitors", ta.float64),("wkends", ta.float64),("seasonality", ta.float64),("incidentRate", ta.float64), ("holidayFlag", ta.float64),("postHolidayFlag", ta.float64),("mintemp", ta.float64)]
        csv = ta.CsvFile("/datasets/arx_train.csv", schema=schema, skip_header_lines=1)

        print "create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArimaxModel object"
        arimax = ta.ArimaxModel()

        print "Training the model on the Frame"
        coefficients = arimax.train(train_frame, "y", ["visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp"], 1, 1, 1, 1, False)

        expected_coefficients = [{u'ar': [-0.017383421475889338],
                                  u'c': 0.12592884652020608,
                                  u'ma': [-0.9175172681125372],
                                  u'xreg': [0.026948785665512696,
                                            -0.27509641527217865,
                                            -30.04399435207178,
                                            0.23517811763216095,
                                            6.6167555198084225,
                                            0.8706683776904101,
                                            0.20954216832984773]}]

        self.assertEqual(coefficients, expected_coefficients)


        print "create test frame"
        csv2 = ta.CsvFile("/datasets/arx_test.csv", schema=schema, skip_header_lines=1)
        test_frame = ta.Frame(csv2)

        print "Predicting on the Frame"
        p = arimax.predict(test_frame, "y", ["visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp"])
        self.assertEqual(p.column_names, ["y","visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp","predicted_y"])

        expected_results = [[[107.08170082770327],
                             [106.14721021492875],
                             [105.66075720064653],
                             [104.95697359162283],
                             [104.88559443691666],
                             [105.85093277068711],
                             [106.1696229024144],
                             [106.70909616071428],
                             [105.35885193790156],
                             [105.71779481717215],
                             [107.09473701831531],
                             [106.14901905655587],
                             [107.12955639032205],
                             [107.4479823114264],
                             [107.56837582595121]]]

        self.assertEqual(expected_results, p.take(p.row_count, 1, "predicted_y"))


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
