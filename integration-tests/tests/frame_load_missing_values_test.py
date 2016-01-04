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
    Test loads data with missing values, verifies that the load passes and then checks
    that we are able to add_column (i.e. to swap missing values for another value) and
    drop_rows (in case we want to just get rid of rows with missing values)
    """
    _multiprocess_can_split_ = True

    def test_missing_values_load_from_csv(self):
        # Test that we can load from a csv file that has missing values
        schema = [('a', ta.int32), ('b', ta.int64), ('c', ta.float32), ('d', ta.float64)]
        csv = ta.CsvFile("/datasets/missing_values.csv", schema=schema)
        frame = ta.Frame(csv, schema)

        # Check row count
        self.assertEqual(6, frame.row_count)

        # Check frame values
        expected_value = [[1,2,None,3.5],
                          [4,None,None,7.0],
                          [None,None,None,None],
                          [None,75,4.5,None],
                          [10,20,30.0,40.5],
                          [None,None,None,None]]
        self.assertEqual(expected_value, frame.take(frame.row_count))

    def test_upload_rows_with_missing_values(self):
        # Test that we can create a frame with missing values using UploadRows
        data = [[1],[None],[5],[7],[None]]
        schema = [('a', ta.int32)]
        frame = ta.Frame(ta.UploadRows(data, schema))

        # Check row count
        self.assertEqual(5, frame.row_count)

        # Check frame values
        self.assertEquals(data, frame.take(frame.row_count))


if __name__ == "__main__":
    unittest.main()