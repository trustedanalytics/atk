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

class FrameCopyTest(unittest.TestCase):
    """
    Test copy()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_copy_001(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', ta.int32),
                                            ('city', str),
                                            ('population_2013', str),
                                            ('pop_2010', str),
                                            ('change', str),
                                            ('county', str)], delimiter='|')

        print "create frame"
        frame = ta.Frame(csv)

        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        self.assertEqual(frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])

        print "copy()"
        top10_frame = frame.copy()
        self.assertEquals(top10_frame.row_count, 20, "copy should have same number of rows as original")
        self.assertNotEquals(frame.uri, top10_frame.uri, "copy should have a different id from the original")

    def test_copy_empty(self):
        csv = ta.CsvFile("/datasets/empty.csv", schema= [('rank', ta.int32),
                                                                 ('city', str),
                                                                 ('population_2013', str),
                                                                 ('pop_2010', str),
                                                                 ('change', str),
                                                                 ('county', str)], delimiter='|')
        print "create frame"
        f1 = ta.Frame(csv)
        f1.inspect()

        print "copy frame"
        f2 = f1.copy()
        f2.inspect()

        self.assertEquals(f1.row_count, 0)
        self.assertEquals(f2.row_count, 0)

if __name__ == "__main__":
    unittest.main()
