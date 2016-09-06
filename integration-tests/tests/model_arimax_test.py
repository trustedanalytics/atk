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

        expected_results = [[106.28884655162547],
                            [106.41294395514359],
                            [106.53671556419438],
                            [106.66049283666771],
                            [106.78427001069139],
                            [106.90804718642644],
                            [107.03182436213176],
                            [107.15560153783758],
                            [107.2793787135434],
                            [107.4031558892492],
                            [107.52693306495502],
                            [107.65071024066083],
                            [107.77448741636664],
                            [107.89826459207245],
                            [108.02204176777826]]

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
        arimax.train(train_frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"], 2, 1, 2, 1, False, False)

        print "Create test frame"
        csv2 = ta.CsvFile("/datasets/arimax_test.csv", schema=schema, skip_header_lines=1)
        test_frame = ta.Frame(csv2)

        print "Predicting on the Frame"
        p = arimax.predict(test_frame, "CO_GT", ["C6H6_GT", "PT08_S2_NMHC", "T"])

        expected_results = [[3.9, 2.8479650290015357],
                            [3.7, 4.01869523508007],
                            [6.6, 2.8942435703448774],
                            [4.4, 3.784738416089903],
                            [3.5, 3.1015558917320334],
                            [5.4, 3.622582716071004],
                            [2.7, 3.225675609437958],
                            [1.9, 3.527964749324494],
                            [1.6, 3.29774747713215],
                            [1.7, 3.4730748547747217],
                            [-200.0, 3.3395503758570433],
                            [1.0, 3.4412388933317657],
                            [1.2, 3.3637957602754716],
                            [1.5, 3.422774285477817],
                            [2.7, 3.377857889532884],
                            [3.7, 3.4120649609529656],
                            [3.2, 3.38601381169157],
                            [4.1, 3.4058536386529243],
                            [3.6, 3.3907441811044],
                            [2.8, 3.4022511215558073]]

        self.assertEqual(expected_results, p.take(20, columns=["CO_GT", "predicted_y"]))


if __name__ == "__main__":
    unittest.main()
