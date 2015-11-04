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
if ta.server.port != 9099:
    ta.server.port = 9099
ta.connect()

class FrameLoadMissingValuesTest(unittest.TestCase):
    """
    Test uses a csv file that has missing values and tests loading a frame with different datatypes.
    Ensures that the integer/float option datatype columns are able to handle missing values and
    have a proper missing value in the frame.


    missing_values_test data looks like:
    1	 2	  None  [12,3]
    3	 4	  5	    [6,28]
    22	 None 37	[8,5]
    7	 2	  None  [12,2]
    8	 9	  4	    [40,4]
    bad	 None None  [6,50]
    0	 bad  0	    [55,7]
    0	 0	  3.5   [10,5]

    The "bad" rows should not load into the frame since strings can't be loaded into integer columns.
    The "3.5" row should also not load if the column is an integer data type (but should load if the
    column is using a float data type).

    """
    _multiprocess_can_split_ = True

    def setUp(self):
        self.csv_path = "missing_values_test.csv"

    def test_missing_values_using_int32option_datatype(self):
        # Tests loading a csv file with missing values to a frame using the int32option datatype
        schema = [('a', ta.int32),('b', ta.int32option),('c', ta.int32option),('d',ta.vector(2))]
        csv = ta.CsvFile(self.csv_path,schema=schema,delimiter=":")
        frame = ta.Frame(csv)
        self.assertEqual(5, frame.row_count)

        expected_value = [[1,2,None,[12,3]],
                          [3,4,5,[6,28]],
                          [22,None,37,[8,5]],
                          [7,2,None,[12,2]],
                          [8,9,4,[40,4]]]

        # check frame values
        self.assertEqual(expected_value, frame.take(frame.row_count))

        # column summary statistics on column 'b'
        stats = frame.column_summary_statistics('b')
        self.assertEqual(stats['bad_row_count'], 1)     # one "None" row in column b
        self.assertEqual(stats['good_row_count'], 4)    # four non-"None" rows in column b
        self.assertEqual(stats['mean'], 4.25)           # mean (excluding the "bad" rows)
        self.assertEqual(stats['minimum'],2)            # minimum (excluding the "bad" rows)
        self.assertEqual(stats['maximum'],9)            # maximum (excluding the "bad" rows)

        # append a row
        row_to_add = [[11,None,None,[12,13]]]
        frame.append(ta.UploadRows(row_to_add,schema))
        self.assertEqual(6, frame.row_count)
        added_row = frame.take(1, offset=(frame.row_count-1))
        self.assertEqual(row_to_add, added_row)

        # bin column
        frame.bin_column('b',[1,5,10])
        bin_column = 4      # index to the binned column
        expected_bins = [0,0,-1,0,1,-1]
        count = 0
        for row in frame.take(frame.row_count):
            self.assertEqual(expected_bins[count], row[bin_column])
            count += 1

    def test_missing_values_using_int64option_datatype(self):
        # Tests loading a csv file with missing values to a frame using the int64option datatype
        schema = [('a', ta.int32),('b', ta.int64option),('c', ta.int64option),('d',ta.vector(2))]
        csv = ta.CsvFile(self.csv_path,schema=schema,delimiter=":")
        frame = ta.Frame(csv)
        self.assertEqual(5, frame.row_count)

        expected_value = [[1,2,None,[12,3]],
                          [3,4,5,[6,28]],
                          [22,None,37,[8,5]],
                          [7,2,None,[12,2]],
                          [8,9,4,[40,4]]]

        # check frame values
        self.assertEqual(expected_value, frame.take(frame.row_count))

        # column summary statistics on column 'b'
        stats = frame.column_summary_statistics('b')
        self.assertEqual(stats['bad_row_count'], 1)     # one "None" row in column b
        self.assertEqual(stats['good_row_count'], 4)    # four non-"None" rows in column b
        self.assertEqual(stats['mean'], 4.25)           # mean (excluding the "bad" rows)
        self.assertEqual(stats['minimum'],2)            # minimum (excluding the "bad" rows)
        self.assertEqual(stats['maximum'],9)            # maximum (excluding the "bad" rows)

    def test_missing_values_using_float32option_datatype(self):
        # Tests loading a csv file with missing values to a frame using the float32option datatype
        schema = [('a', ta.int32),('b', ta.float32option),('c', ta.float32option),('d',ta.vector(2))]
        csv = ta.CsvFile(self.csv_path,schema=schema,delimiter=":")
        frame = ta.Frame(csv)
        self.assertEqual(6, frame.row_count)

        expected_value = [[1,2,None,[12,3]],
                          [3,4,5,[6,28]],
                          [22,None,37,[8,5]],
                          [7,2,None,[12,2]],
                          [8,9,4,[40,4]],
                          [0,0,3.5,[10,5]]]

        # check frame values
        self.assertEqual(expected_value, frame.take(frame.row_count))

        # column summary statistics on column 'b'
        stats = frame.column_summary_statistics('b')
        self.assertEqual(stats['bad_row_count'], 1)     # one "None" row in column b
        self.assertEqual(stats['good_row_count'], 5)    # five non-"None" rows in column b
        self.assertEqual(stats['mean'], 3.4)            # mean (excluding the "bad" rows)
        self.assertEqual(stats['minimum'],0)            # minimum (excluding the "bad" rows)
        self.assertEqual(stats['maximum'],9)            # maximum (excluding the "bad" rows)

    def test_missing_values_using_float64option_datatype(self):
        # Tests loading a csv file with missing values to a frame using the float64option datatype
        schema = [('a', ta.int32),('b', ta.float64option),('c', ta.float64option),('d',ta.vector(2))]
        csv = ta.CsvFile(self.csv_path,schema=schema,delimiter=":")
        frame = ta.Frame(csv)
        self.assertEqual(6, frame.row_count)

        expected_value = [[1,2,None,[12,3]],
                          [3,4,5,[6,28]],
                          [22,None,37,[8,5]],
                          [7,2,None,[12,2]],
                          [8,9,4,[40,4]],
                          [0,0,3.5,[10,5]]]

        # check frame values
        self.assertEqual(expected_value, frame.take(frame.row_count))

        # column summary statistics on column 'b'
        stats = frame.column_summary_statistics('b')
        self.assertEqual(stats['bad_row_count'], 1)     # one "None" row in column b
        self.assertEqual(stats['good_row_count'], 5)    # five non-"None" rows in column b
        self.assertEqual(stats['mean'], 3.4)            # mean (excluding the "bad" rows)
        self.assertEqual(stats['minimum'],0)            # minimum (excluding the "bad" rows)
        self.assertEqual(stats['maximum'],9)            # maximum (excluding the "bad" rows)

    def test_missing_values_using_non_option_datatype(self):
        # When a non-option datatype is used a values are missing, we expect this to fail
        schema = [('a', ta.int32),('b', ta.int32),('c', ta.int32),('d',ta.vector(2))]
        csv = ta.CsvFile(self.csv_path,schema=schema,delimiter=":")
        with self.assertRaises(Exception):
            ta.Frame(csv)


if __name__ == "__main__":
    unittest.main()
