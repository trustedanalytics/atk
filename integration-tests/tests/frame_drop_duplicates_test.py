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


class FrameDropDuplicatesTest(unittest.TestCase):
    """
    Test frame.drop_duplicates()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        print "define csv file"
        csv = atk.CsvFile("/datasets/duplicates.csv", schema= [('rank', atk.int32),
                                                                 ('city', str),
                                                                 ('population_2013', str),
                                                                 ('pop_2010', str),
                                                                 ('change', str),
                                                                 ('county', str)], delimiter='|')

        print "create frame"
        self.frame = atk.Frame(csv)

    def test_drop_duplicates(self):
        self.assertEqual(self.frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county'])
        self.assertEqual(self.frame.row_count, 14)
        self.frame.drop_duplicates()
        self.assertEqual(self.frame.row_count, 6)


if __name__ == "__main__":
    unittest.main()
