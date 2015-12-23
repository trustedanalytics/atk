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

    def setUp(self):
        data = [[10],[2],[None],[47],[6],[None],[22],[4],[None]]
        schema = [('a', ta.int32)]
        self.frame = ta.Frame(ta.UploadRows(data, schema))

    def test_missing_values_add_column(self):
        # Check that we can use add columns to replace missing values

        # Define function that replaces missing values with zero
        def noneToZero(x):
            if x is None:
                return 0
            else:
                return x

        self.frame.add_columns(lambda row: noneToZero(row['a']), ('a_corrected', ta.int32), columns_accessed='a')
        expected = [[10],[2],[0],[47],[6],[0],[22],[4],[0]]
        self.assertEqual(expected, self.frame.take(self.frame.row_count, columns='a_corrected'))


    def test_missing_values_drop_rows(self):
        # Check that we can drop rows with missing values
        self.frame.drop_rows(lambda row: row['a'] == None)
        expected = [[10],[2],[47],[6],[22],[4]]
        self.assertEqual(expected, self.frame.take(self.frame.row_count, columns='a'))

if __name__ == "__main__":
    unittest.main()