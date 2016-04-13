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


class FrameAppendTests(unittest.TestCase):
    """
    Test frame.append

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        self.schema1 = [('rank', ta.int32),
                        ('city', str),
                        ('population_2013', str),
                        ('pop_2010', str),
                        ('change', str),
                        ('county', str)]
        self.schema2 = [('number', ta.int32),
                        ('abc', str),
                        ('food', str)]
        self.combined_schema = []
        self.combined_schema.extend(self.schema1)
        self.combined_schema.extend(self.schema2)

        self.csv1 = ta.CsvFile("/datasets/oregon-cities.csv", schema=self.schema1, delimiter='|',skip_header_lines=1)
        self.csv2 = ta.CsvFile("/datasets/flattenable.csv", schema= self.schema2, delimiter=',',skip_header_lines=1)

    def test_append_to_empty_frame(self):
        frame = ta.Frame()
        self.assertEqual(frame.row_count, 0)
        self.assertEqual(frame.column_names, [])

        frame.append(self.csv1)
        self.assertEqual(frame.row_count, 20)
        self.assertEqual(frame.column_names, [name for name, type in self.schema1])

    def test_append_same_schema(self):
        frame = ta.Frame(self.csv1)
        self.assertEqual(frame.row_count, 20)
        self.assertEqual(frame.column_names, [name for name, type in self.schema1])
        frame.append(self.csv1)
        self.assertEqual(frame.row_count, 40)
        self.assertEqual(frame.column_names, [name for name, type in self.schema1])

    def test_append_new_columns(self):
        frame = ta.Frame(self.csv1)
        self.assertEqual(frame.row_count, 20)
        self.assertEqual(frame.column_names, [name for name, type in self.schema1])
        frame.append(self.csv2)
        self.assertEqual(frame.row_count, 30)
        self.assertEqual(frame.column_names, [name for name, type in self.combined_schema])

    def test_append_frame(self):
        src_frame = ta.Frame(self.csv1)
        self.assertEqual(src_frame.row_count, 20)
        self.assertEqual(src_frame.column_names, [name for name, type in self.schema1])

        dest_frame = ta.Frame(self.csv2)
        self.assertEqual(dest_frame.row_count, 10)
        self.assertEqual(dest_frame.column_names,[name for name, type in  self.schema2])

        src_frame.append(dest_frame)
        self.assertEqual(src_frame.row_count, 30)
        self.assertEqual(src_frame.column_names, [name for name, type in self.combined_schema])




if __name__ == "__main__":
    unittest.main()
