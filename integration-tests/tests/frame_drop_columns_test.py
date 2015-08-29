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


class FrameDropColumnsTest(unittest.TestCase):
    """
    Test frame.drop_columns()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        print "define csv file"
        csv = atk.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', atk.int32),
                                                             ('city', str),
                                                             ('population_2013', str),
                                                             ('pop_2010', str),
                                                             ('change', str),
                                                             ('county', str)], delimiter='|')

        print "create frame"
        self.frame = atk.Frame(csv)

    def test_drop_first_column(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns("rank")
        self.assertEqual(self.frame.column_names, ['city', 'population_2013', 'pop_2010', 'change', 'county'])

    def test_drop_second_column(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns("city")
        self.assertEqual(self.frame.column_names, ['rank', 'population_2013', 'pop_2010', 'change', 'county'])

    def test_drop_last_column(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns("county")
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change'])

    def test_drop_multiple_columns_001(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns(['rank', 'pop_2010'])
        self.assertEqual(self.frame.column_names, ['city', 'population_2013', 'change', 'county'])

    def test_drop_multiple_columns_002(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.frame.drop_columns(['city', 'rank', 'pop_2010', 'change'])
        self.assertEqual(self.frame.column_names, ['population_2013', 'county'])

if __name__ == "__main__":
    unittest.main()
