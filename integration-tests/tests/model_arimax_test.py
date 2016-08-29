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
    def test_arimax_no_lags(self):
        print "define csv file"
        schema = [("y", ta.float64),("visitors", ta.float64),("wkends", ta.float64),("seasonality", ta.float64),("incidentRate", ta.float64), ("holidayFlag", ta.float64),("postHolidayFlag", ta.float64),("mintemp", ta.float64)]
        csv = ta.CsvFile("/datasets/arx_train.csv", schema=schema, skip_header_lines=1)

        print "create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArimaxModel object"
        arimax = ta.ArimaxModel()

        print "Training the model on the Frame"
        m = arimax.train(train_frame, "y", ["visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp"], 1, 1, 1, 0, True)

        expected_coefficients = [{u'ar': [-0.017574956037210644],
                                  u'c': 0.12555666417755332,
                                  u'ma': [-0.9175428858996906],
                                  u'xreg': [2.6856226861751337e-08,
                                            1.6913274131242466e-06,
                                            15238.142921221388,
                                            2.8654054267070586e-07,
                                            4.860690264967836e-07,
                                            -1.8374216542147503e-06,
                                            2.416210415270103e-07]}
                                 ]

        self.assertEqual(m, expected_coefficients)


        print "create test frame"
        csv2 = ta.CsvFile("/datasets/arx_test.csv", schema=schema, skip_header_lines=1)
        test_frame = ta.Frame(csv2)

        print "Predicting on the Frame"
        p = arimax.predict(test_frame, "y", ["visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp"])
        self.assertEqual(p.column_names, ["y","visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp","predicted_y"])

        expected_results = [[115.48893565801859],
                            [115.28389647706668],
                            [115.6968122002911],
                            [115.3086423204453],
                            [116.7218798474326],
                            [115.43195223173811],
                            [115.44677515508538],
                            [114.2593040359367],
                            [115.87394727940287],
                            [117.08366427400865],
                            [115.69409393002961],
                            [118.10909177065538],
                            [117.317403513559],
                            [116.53135116655103],
                            [116.74528764137638]]

        self.assertEqual(expected_results, p.take(p.row_count, 1, "predicted_y"))

    def test_arimax_with_lag(self):
        print "define csv file"
        schema = [("y", ta.float64),("visitors", ta.float64),("wkends", ta.float64),("seasonality", ta.float64),("incidentRate", ta.float64), ("holidayFlag", ta.float64),("postHolidayFlag", ta.float64),("mintemp", ta.float64)]
        csv = ta.CsvFile("/datasets/arx_train.csv", schema=schema, skip_header_lines=1)

        print "create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArimaxModel object"
        arimax = ta.ArimaxModel()

        print "Training the model on the Frame with p=1, d=1, q=1, xMaxLag=1"
        coefficients = arimax.train(train_frame, "y", ["visitors","wkends","seasonality","incidentRate","mintemp"], 1, 1, 1, 1, True)
        self.assertEqual(coefficients['c'], 0.1255562898081476)
        self.assertEqual(coefficients['ar'], [-0.017575034130881045])
        self.assertEqual(coefficients['ma'], [-0.9175428971322549])
        self.assertEqual(coefficients['xreg'], [-7.873733393546116e-09,
                                               -1.2205647089417771e-08,
                                               0.000766029602483627,
                                               4.592819482753666e-08,
                                               -1.0927108582888228e-07,
                                               2.8556704403623667e-08,
                                               1.9039918852705696e-06,
                                               15238.143012203698,
                                               2.509924549203801e-07,
                                               2.122053751579772e-07])

        print "create test frame"
        csv = ta.CsvFile("/datasets/arx_test.csv", schema=schema, skip_header_lines=1)
        test_frame = ta.Frame(csv)

        print "Predicting on the Frame"
        p = arimax.predict(test_frame, "y", ["visitors","wkends","seasonality","incidentRate","mintemp"])
        self.assertEqual(p.column_names, ["y","visitors","wkends","seasonality","incidentRate","holidayFlag","postHolidayFlag","mintemp","predicted_y"])

        expected_results = [[105.55408982267579],
                            [105.56567132520082],
                            [105.57820656626446],
                            [105.5907402301133],
                            [105.60327385798291],
                            [105.61580759047929],
                            [105.62834113774522],
                            [105.64087466062102],
                            [105.65340825398481],
                            [105.66594187906668],
                            [105.67847553027053],
                            [105.69100910906367],
                            [105.7035427046958],
                            [105.71607637379999],
                            [105.72860990348767],
                            [105.7411434939925]]

        self.assertEqual(expected_results, p.take(p.row_count, 0, "predicted_y"))

    def test_arimax_pov(self):
        print "define csv file"
        schema = [("SUPPLIER", str),("REF", ta.float64),("DATE", str),("VALUE", ta.float64),("converusd", ta.float64), ("petrole", ta.float64),("petrole_sin", ta.float64),("petrole_log", ta.float64)]
        csv = ta.CsvFile("/datasets/arimax_train.csv", schema=schema, skip_header_lines=1)

        print "create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArimaxModel object"
        arimax = ta.ArimaxModel()

        print "Training the model on the Frame"
        coeff = arimax.train(train_frame, "VALUE", ["converusd", "petrole"], 1, 1, 1, 0, True)

        self.assertEqual(coeff['c'], 0.003381525295695335)
        self.assertEqual(coeff['ar'], [0.7332052419145086])
        self.assertEqual(coeff['ma'], [-0.9637250701387984])
        self.assertEqual(coeff['xreg'], [-2.8629504307261073, -0.010396258294983547])

        print "create test frame"
        csv2 = ta.CsvFile("/datasets/arimax_test.csv", schema=schema, skip_header_lines=1)
        test_frame = ta.Frame(csv2)

        print "Predicting on the Frame"
        p = arimax.predict(test_frame, "VALUE", ["converusd","petrole"])
        self.assertEqual(p.column_names, ["SUPPLIER","REF","DATE","VALUE","converusd","petrole","petrole_sin","petrole_log","predicted_y"])

        expected_results = [[41.66384743319339],
                            [41.298961808947716],
                            [41.27251949012402],
                            [41.26850807273888],
                            [41.265910023852236],
                            [41.26617299997271],
                            [41.268592554239326],
                            [41.26910810975603],
                            [41.26948406308634],
                            [41.270954343361005]]

        self.assertEqual(expected_results, p.take(p.row_count, 0, "predicted_y"))


if __name__ == "__main__":
    unittest.main()
