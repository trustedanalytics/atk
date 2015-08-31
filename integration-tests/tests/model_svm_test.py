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

class ModelSvmTest(unittest.TestCase):
    def testSvm(self):
        print "define csv file"
        csv = atk.CsvFile("/datasets/SvmTestFile.csv", schema= [('data', atk.float64),
                                                                  ('label', str)], skip_header_lines=1)

        print "create frame"
        frame = atk.Frame(csv)

        print "Initializing a SvmModel object"
        k = atk.SvmModel(name='mySvmModel')

        print "Training the model on the Frame"
        k.train(frame,'label', ['data'])

        print "Predicting on the Frame"
        m = k.predict(frame)

        self.assertEqual(m.column_names, ['data', 'label', 'predicted_label'])


if __name__ == "__main__":
    unittest.main()
