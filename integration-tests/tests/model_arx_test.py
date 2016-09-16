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
    # ARX tests using air quality data in arx_train.csv and arx_test.csv
    # Data sets are snippets from https://archive.ics.uci.edu/ml/datasets/Air+Quality
    schema = [("Date", str),
              ("Time",str),
              ("CO_GT", ta.float32),
              ("PT08_S1_CO", ta.int32),
              ("NMHC_GT", ta.int32),
              ("C6H6_GT", ta.float32),
              ("PT08_S2_NMHC", ta.int32),
              ("NOx_GT",ta.int32),
              ("PT08_S3_NOx",ta.int32),
              ("NO2_GT", ta.int32),
              ("PT08_S4_NO2",ta.int32),
              ("PT08_S5_O3", ta.int32),
              ("Temp", ta.float32),
              ("RH", ta.float32),
              ("AH", ta.float32)]
    # column to predict (temperature)
    y = "Temp"
    # exogenous variable columns
    x_columns = ["CO_GT", "PT08_S1_CO", "NMHC_GT", "C6H6_GT", "PT08_S2_NMHC", "NOx_GT", "PT08_S3_NOx", "NO2_GT", "PT08_S4_NO2", "PT08_S5_O3"]

    def test_arx_no_lags(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/arx_train.csv", schema=self.schema, skip_header_lines=3, delimiter=";")

        print "create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArxModel object"
        arx = ta.ArxModel()

        print "Training the model on the Frame"
        arx.train(train_frame, self.y, self.x_columns,0,0,True)

        print "create test frame"
        csv = ta.CsvFile("/datasets/arx_test.csv", schema=self.schema, skip_header_lines=3, delimiter=";")
        test_frame = ta.Frame(csv)

        print "Predicting on the Frame"
        p = arx.predict(test_frame, self.y, self.x_columns)
        self.assertTrue(self.y in p.column_names)
        self.assertTrue("predicted_y" in p.column_names)
        for col in self.x_columns:
            self.assertTrue(col in p.column_names)

        expected_results = [[5.848812761346309],
                            [7.43541307830168],
                            [9.074815060209268],
                            [11.362016537415789],
                            [13.497976194856989],
                            [15.420507102497428],
                            [13.723555762790738],
                            [13.797255196045285],
                            [14.044904861592656],
                            [12.827091332570216],
                            [12.457343737409357],
                            [12.768543378067044],
                            [12.199162646736658],
                            [11.166481544975762],
                            [9.46077382447892],
                            [10.817842993541653],
                            [10.672801682561026],
                            [11.805866879076122],
                            [10.487865138143718],
                            [11.843052200270032],
                            [7.3282014468144725],
                            [7.106231735627839],
                            [6.397662234463189],
                            [4.407664995382392],
                            [6.459641204827768],
                            [6.636215244988309],
                            [8.525909897183531],
                            [9.383998719921518],
                            [10.785742148450698],
                            [11.505509931987097],
                            [12.362666587410716],
                            [13.161845000529247],
                            [14.599170184838016],
                            [12.998252001810606],
                            [13.232329771677616],
                            [14.071213186651079],
                            [12.341259407104511],
                            [10.991703990712274],
                            [9.08635252144675],
                            [8.143625744411672],
                            [8.294921121909741],
                            [7.937574074073086]]

        self.assertEqual(expected_results, p.take(p.row_count, 0, "predicted_y"))

    def test_arx_with_lag(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/arx_train.csv", schema=self.schema, skip_header_lines=3, delimiter=";")

        print "create training frame"
        train_frame = ta.Frame(csv)

        print "Initializing a ArxModel object"
        arx = ta.ArxModel()

        print "Training the model on the Frame with yMaxLag = 2 and xMaxLag = 2"
        coefficients = arx.train(train_frame, self.y, self.x_columns,2,2,True)
        self.assertEqual(coefficients['coefficients'], [0.9621588921826002,
                                                        0.32339681234786966,
                                                        -0.01473434325696366,
                                                        -0.0023029745191882832,
                                                        -0.007644418017073593,
                                                        -0.002312917244877265,
                                                        -0.01681921225458392,
                                                        -0.005459828800337797,
                                                        -0.3300806739075168,
                                                        -0.020030500261562254,
                                                        -0.011499084028172187,
                                                        0.01608010721596393,
                                                        0.016938493891060218,
                                                        -0.0033384353524076806,
                                                        -0.0008357578107839751,
                                                        0.008627541827496329,
                                                        -0.0036315825364330806,
                                                        0.006326116380660007,
                                                        0.023462113708708383,
                                                        -0.007572916846853751,
                                                        0.0026437346952873095,
                                                        0.004485920671000246,
                                                        -0.017752207004818044,
                                                        0.002189306026607933,
                                                        -0.005145279988557949,
                                                        0.6036385601049182,
                                                        -0.015629406718667988,
                                                        0.025310178712328248,
                                                        -0.008739650886181416,
                                                        -0.025767148806180556,
                                                        -0.0059371750243069335,
                                                        -0.007475424442827556])

        print "create test frame"
        csv = ta.CsvFile("/datasets/arx_test.csv", schema=self.schema, skip_header_lines=3, delimiter=";")
        test_frame = ta.Frame(csv)

        print "Predicting on the Frame"
        p = arx.predict(test_frame, self.y, self.x_columns)
        self.assertTrue(self.y in p.column_names)
        self.assertTrue("predicted_y" in p.column_names)
        for col in self.x_columns:
            self.assertTrue(col in p.column_names)

        expected_results = [[None],
                            [None],
                            [8.546281135843117],
                            [11.069175189357603],
                            [14.440712298227151],
                            [17.857047251825186],
                            [20.250512203833182],
                            [18.897730012282764],
                            [18.11214405165865],
                            [17.76612657433568],
                            [17.780455094900866],
                            [16.16165010408777],
                            [15.269548500742825],
                            [14.394703463792764],
                            [14.067985900637495],
                            [16.674109154225498],
                            [14.940918715691119],
                            [14.119125988705331],
                            [13.764782418703584],
                            [12.52731597400307],
                            [10.193701632861206],
                            [14.167318906650085],
                            [12.004844831528448],
                            [12.185505125998706],
                            [12.148991638691303],
                            [12.278240287212695],
                            [13.206542073814422],
                            [13.912894970688889],
                            [16.678389996653983],
                            [20.079305598203202],
                            [22.41857686545336],
                            [22.849466210939006],
                            [22.87286398151099],
                            [22.2889155924812],
                            [22.467522560809257],
                            [19.84268096320567],
                            [17.991846514057848],
                            [17.012040287342757],
                            [17.23857188165568],
                            [17.748262739811985],
                            [17.035464668899962],
                            [16.12688239522264]]

        self.assertEqual(expected_results, p.take(p.row_count, 0, "predicted_y"))

if __name__ == "__main__":
    unittest.main()
