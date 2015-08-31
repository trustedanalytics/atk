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

class ModelPrincipalComponentsTest(unittest.TestCase):
    def test_principal_components(self):
        print "define csv file"
        schema= [("1", atk.float64),("2", atk.float64),("3", atk.float64),("4", atk.float64),("5", atk.float64),("6", atk.float64),
                 ("7", atk.float64),("8", atk.float64),("9", atk.float64),("10", atk.float64),("11", atk.float64)]
        train_file = atk.CsvFile("/datasets/pca_10rows.csv", schema= schema)
        print "creating the frame"
        train_frame = atk.Frame(train_file)

        print "initializing the naivebayes model"
        p = atk.PrincipalComponentsModel()

        print "training the model on the frame"
        p.train(train_frame,["1","2","3","4","5","6","7","8","9","10","11"],9)

        print "predicting the class using the model and the frame"
        output = p.predict(train_frame,c=5,t_square_index=True)
        output_frame = output['output_frame']

        self.assertEqual(output_frame.column_names,['1','2','3','4','5','6','7','8','9','10','11','p_1','p_2','p_3','p_4','p_5'])


if __name__ == "__main__":
    unittest.main()
