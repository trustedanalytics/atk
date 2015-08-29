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
atk.connect()()

class FrameSmokeTest(unittest.TestCase):
    """
    Smoke test basic frame operations to verify functionality that will be needed by all other tests.

    If these tests don't pass, there is no point in running other tests.

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_frame(self):
        print "define csv file"
        csv = atk.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', atk.int32),
                                            ('city', str),
                                            ('population_2013', str),
                                            ('pop_2010', str),
                                            ('change', str),
                                            ('county', str)], delimiter='|')

        print "create frame"
        frame = atk.Frame(csv)

        # TODO: add asserts verifying inspect is working
        print
        print frame.inspect(20)
        print
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        self.assertGreaterEqual(frame._size_on_disk, 0, "frame size on disk should be non-negative")
        self.assertEqual(frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.assertEquals(len(frame.column_names), 6)

        print "get_error_frame()"
        error_frame = frame.get_error_frame()

        # TODO: add asserts verifying inspect is working

        print
        print error_frame.inspect(20)
        print
        self.assertEquals(error_frame.row_count, 2, "error frame should have 2 bad rows after loading")
        self.assertEquals(len(error_frame.column_names), 2, "error frames should have 2 columns (original value and error message)")

        # TODO: add verification that one Python UDF is working (not working yet)


if __name__ == "__main__":
    unittest.main()
