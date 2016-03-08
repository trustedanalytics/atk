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


class FrameSortTest(unittest.TestCase):
    """
    Test frame.sort()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', ta.int32),
                                                                 ('city', str),
                                                                 ('population_2013', str),
                                                                 ('pop_2010', str),
                                                                 ('change', str),
                                                                 ('county', str)], delimiter='|',skip_header_lines=1)

        print "create frame"
        self.frame = ta.Frame(csv)

    def test_frame_ascending_sort(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.sort('rank')
        print self.frame.inspect(30)
        results = self.frame.take(20)
        self.assertEquals(results[0][0], 1)
        self.assertEquals(results[9][0], 10)
        self.assertEquals(results[19][0], 20)

    def test_frame_descending_sort(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.sort('rank', False)
        print self.frame.inspect(30)
        results = self.frame.take(20)
        self.assertEquals(results[0][0], 20)
        self.assertEquals(results[9][0], 11)
        self.assertEquals(results[19][0], 1)

if __name__ == "__main__":
    unittest.main()
