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

class FrameColumnSummaryStatisticsTest(unittest.TestCase):
    def testColumnSummaryStatistics(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/dates.csv", schema= [('start', ta.datetime), ('id', int), ('stop', ta.datetime),
                                                                ('color', str)], delimiter=',')

        print "create frame"
        frame = ta.Frame(csv)

        print "Calling column summary statistics"
        stats = frame.column_summary_statistics("id")
        self.assertEqual(stats['maximum'], 4.0)
        self.asserEqual(stats['variance'], 2.5)



if __name__ == "__main__":
    unittest.main()
