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


import sys
print "path=%s" % sys.path
import unittest
import trustedanalytics as ta

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class ModelLassoTest(unittest.TestCase):

    def test_lasso(self):

        print "create frame"
        frame = ta.Frame(ta.CsvFile("/datasets/lasso_lpsa.csv", schema=[
            ('y', ta.float64),
            ('x1', ta.float64),
            ('x2', ta.float64),
            ('x3', ta.float64),
            ('x4', ta.float64),
            ('x5', ta.float64),
            ('x6', ta.float64),
            ('x7', ta.float64),
            ('x8', ta.float64)], delimiter=' '))

        model = ta.LassoModel()
        model.train(frame, 'y', ['x1','x2','x3','x4','x5','x6','x7','x8'])

        #print repr(train_output)

        predicted_frame = model.predict(frame)
        print predicted_frame.inspect(20, columns=['y', 'predicted_value'])

        test_metrics = model.test(predicted_frame, 'predicted_value')

        print str(test_metrics)
