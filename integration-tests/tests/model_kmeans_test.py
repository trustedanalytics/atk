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
import trustedanalytics as ta

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class ModelKMeansTest(unittest.TestCase):
    def testKMeans(self):
        """basic KMeans train + piggyback model last_read_date"""
        print "define csv file"
        csv = ta.CsvFile("/datasets/KMeansTestFile.csv", schema= [('data', ta.float64),
                                                             ('name', str)], skip_header_lines=1)

        print "create frame"
        frame = ta.Frame(csv)

        print "Initializing a KMeansModel object"
        k = ta.KMeansModel(name='myKMeansModel')
        t0 = k.last_read_date
        t1 = k.last_read_date
        #print "t0=%s" % t0.isoformat()
        self.assertEqual(t0, t1)

        print "Training the model on the Frame"
        k.train(frame,['data'],[2.0])
        t2 = k.last_read_date
        self.assertLess(t1, t2)


if __name__ == "__main__":
    unittest.main()
