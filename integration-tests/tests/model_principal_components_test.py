# vim: set encoding=utf-8
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
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class ModelPrincipalComponentsTest(unittest.TestCase):
    def test_principal_components(self):
        print "define csv file"
        schema= [("1", ta.float64),("2", ta.float64),("3", ta.float64),("4", ta.float64),("5", ta.float64),("6", ta.float64),
                 ("7", ta.float64),("8", ta.float64),("9", ta.float64),("10", ta.float64),("11", ta.float64)]
        train_file = ta.CsvFile("/datasets/pca_10rows.csv", schema= schema)
        print "creating the frame"
        train_frame = ta.Frame(train_file)

        print "initializing the principalcomponents model"
        p = ta.PrincipalComponentsModel()

        print "training the model on the frame"
        p.train(train_frame,["1","2","3","4","5","6","7","8","9","10","11"],k=9)

        print "predicting the class using the model and the frame"
        output = p.predict(train_frame,c=5,t_squared_index=True)

        self.assertEqual(output.column_names,['1','2','3','4','5','6','7','8','9','10','11','p_1','p_2','p_3','p_4','p_5','t_squared_index'])


if __name__ == "__main__":
    unittest.main()
