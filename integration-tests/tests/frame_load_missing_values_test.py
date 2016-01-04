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

class FrameLoadMissingValuesTest(unittest.TestCase):
    """
    Tests creating frames with missing values (from a csv file and using UploadRows).  Checks
    that we are able to drop rows with missing values, or use add columns to create a new column
    that replaces missing values with some other value.
    """

    def test_load_csv_with_missing_values(self):
        # Load csv with missing values
        schema = [('a', ta.int32), ('b', ta.int64), ('c', ta.float32), ('d', ta.float64)]
        csv = ta.CsvFile("datasets/missing_values.csv", schema=schema)
        frame = ta.Frame(csv)

        # Check row count
        self.assertEqual(6, frame.row_count)

        # Check expected values
        expected_value = [[1,2,None,3.5],
                          [4,None,None,7.0],
                          [None,None,None,None],
                          [None,75,4.5,None],
                          [10,20,30.0,40.5],
                          [None,None,None,None]]
        self.assertEqual(expected_value, frame.take(frame.row_count))

    def test_missing_values_add_column(self):
        # Create frame with missing values using upload rows
        schema = [('a', ta.int32)]
        data = [[1],[4],[None],[None],[10],[None]]
        frame = ta.Frame(ta.UploadRows(data, schema))

        # Check that frame was correctly created
        self.assertEqual(6, frame.row_count)
        self.assertEqual(data, frame.take(frame.row_count))

        # Define function that replaces missing values with zero
        def noneToZero(x):
            if x is None:
                return 0
            else:
                return x

        # Use add columns to create a new column that replaces missing values with 0.
        frame.add_columns(lambda row: noneToZero(row['a']), ('a_corrected', ta.int32), columns_accessed='a')
        expected = [[1],[4],[0],[0],[10],[0]]
        self.assertEqual(expected, frame.take(frame.row_count, columns='a_corrected'))


    def test_missing_values_drop_rows(self):
        # Create frame with missing values using upload rows
        schema = [('a', ta.int32)]
        data = [[1],[4],[None],[None],[10],[None]]
        frame = ta.Frame(ta.UploadRows(data, schema))

        # Check that frame was correctly created
        self.assertEqual(6, frame.row_count)
        self.assertEqual(data, frame.take(frame.row_count))

        # Check that we can drop rows with missing values
        frame.drop_rows(lambda row: row['a'] == None)
        expected = [[1],[4],[10]]
        self.assertEqual(expected, frame.take(frame.row_count, columns='a'))


if __name__ == "__main__":
    unittest.main()