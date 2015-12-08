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
    Test uses a csv file that has missing values and tests loading a frame with different datatypes.
    Ensures that the integer/float datatype columns are able to handle missing values and
    have a proper missing value in the frame.  Non-numerical values in interger/float columns count
    as missing values.  The only "bad" rows that get excluded from the frame are ones with the wrong
    number of comma separated values.  Those "bad" rows end up in the error frame.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        self.csv_path = "/datasets/missing_values.csv"

    def test_missing_values_using_int_and_float_datatype(self):
        # Tests loading a csv file with missing values to a frame using the int32 datatype
        schema = [('a', ta.int32), ('b', ta.int64), ('c', ta.float32), ('d', ta.float64)]
        csv = ta.CsvFile(self.csv_path,schema=schema)
        frame = ta.Frame(csv)
        self.assertEqual(6, frame.row_count)

        expected_value = [[1,2,None,3.5],
                          [4,None,None,7.0],
                          [None,None,None,None],
                          [None,75,4.5,None],
                          [10,20,30.0,40.5],
                          [None,None,None,None]]

        # check frame values
        self.assertEqual(expected_value, frame.take(frame.row_count))

        # column summary statistics on column 'a'
        stats = frame.column_summary_statistics('a')
        self.assertEqual(stats['bad_row_count'], 3)
        self.assertEqual(stats['good_row_count'], 3)
        self.assertEqual(stats['mean'], 5)
        self.assertEqual(stats['minimum'],1)
        self.assertEqual(stats['maximum'],10)

        # append a row
        row_to_add = [[11,None,None,22.5]]
        frame.append(ta.UploadRows(row_to_add,schema))
        self.assertEqual(7, frame.row_count)
        added_row = frame.take(1, offset=(frame.row_count-1))
        self.assertEqual(row_to_add, added_row)

        # bin column
        frame.bin_column('a',[1,5,12])
        bin_column = 4      # index to the binned column
        expected_bins = [0,0,-1,-1,1,-1,1]
        count = 0
        for row in frame.take(frame.row_count):
            self.assertEqual(expected_bins[count], row[bin_column])
            count += 1

    def test_missing_values_join(self):
        data1 = [[None, "None"],[0, "Zero"],[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]
        schema1 = [('key', ta.int64), ('value', str)]
        data2 = [[3455, None],[9803, None],[1320, 4],[8121, 2],[2027, 1],[5980,None],[3704,3]]
        schema2 = [('a', ta.int32), ('key', ta.int64)]

        frame1 = ta.Frame(ta.UploadRows(data1, schema1))
        frame2 = ta.Frame(ta.UploadRows(data2, schema2))

        joined = frame2.join(frame1, 'key', how='left')
        self.assertEqual(frame2.row_count, joined.row_count)

        expected_values = ["None", "None", "Four", "Two", "One", "None", "Three"]
        actual_values = joined.take(joined.row_count)
        column_index = 3
        for i in range(0, joined.row_count):
            self.assertEqual(expected_values[i], actual_values[i][column_index])

if __name__ == "__main__":
    unittest.main()