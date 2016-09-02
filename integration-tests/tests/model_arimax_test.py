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

        expected_coefficients = [{u'ar': [-0.8981279030037268],
                                  u'c': -0.021231596442917687,
                                  u'ma': [-0.8543855454206947],
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

        expected_results = [[[105.53001433835563],
                             [109.89267337675331],
                             [105.95321596663403],
                             [109.47012099291403],
                             [106.29025886015499],
                             [109.12495017284792],
                             [106.55780321207322],
                             [108.84219793221327],
                             [106.76928769613818],
                             [108.60979462313634],
                             [106.93555239988473],
                             [108.41800446053107],
                             [107.0653413035563],
                             [108.25897423175753],
                             [107.16570759655305]]]

        self.assertEqual(expected_results, p.take(p.row_count, 1, "predicted_y"))


    def test_arimax_air_quality(self):
        print "Define csv file"
        schema = [("Date", str),("Time", str),("CO_GT", ta.float64),("PT08_S1_CO", ta.float64),("NMHC_GT", ta.float64), ("C6H6_GT", ta.float64),("PT08_S2_NMHC", ta.float64),("NOx_GT", ta.float64),
                  ("PT08_S3_NOx", ta.float64),("NO2_GT", ta.float64),("PT08_S4_NO2", ta.float64),("PT08_S5_O3", ta.float64),("T", ta.float64),("RH", ta.float64),("AH", ta.float64)]
        csv = ta.CsvFile("/datasets/arimax_train.csv", schema=schema, skip_header_lines=1)

        print "Create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArimaxModel object"
        arimax = ta.ArimaxModel()

        print "Training the model on the Frame"
        arimax.train(train_frame, "CO_GT", ["C6H6_GT","PT08_S2_NMHC", "T"], 1, 1, 1, 2, False, True)

        print "Create test frame"
        csv2 = ta.CsvFile("/datasets/arimax_test.csv", schema=schema, skip_header_lines=1)
        test_frame = ta.Frame(csv2)

        print "Predicting on the Frame"
        p = arimax.predict(test_frame, "CO_GT", ["C6H6_GT","PT08_S2_NMHC", "T"])

        expected_results = [[3.9, 3.1898858210811287],
                            [3.7, 2.2818847734557384],
                            [6.6, 3.1227047810468007],
                            [4.4, 2.262430500447906],
                            [3.5, 3.056826243795137],
                            [5.4, 2.241709271031164],
                            [2.7, 2.992180086668445],
                            [1.9, 2.21978929415812],
                            [1.6, 2.9286999621926157],
                            [1.7, 2.1967351066256677],
                            [-200.0, 2.8663230948330987],
                            [1.0, 2.1726077707723133],
                            [1.2, 2.8049900886929],
                            [1.5, 2.1474650615321003],
                            [2.7, 2.7446447455635123],
                            [3.7, 2.121361643418137],
                            [3.2, 2.6852338927714148],
                            [4.1, 2.0943492379778874],
                            [3.6, 2.6267072202927744],
                            [2.8, 2.066476782233197]]

        self.assertEqual(expected_results, p.take(20, columns=["CO_GT", "predicted_y"]))


if __name__ == "__main__":
    unittest.main()
