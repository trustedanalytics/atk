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


class FrameBinColumnTest(unittest.TestCase):
    """
    Test all frame binning methods. using the flattenable dataset. all tests bin the number column
    number column goes from 1 to 10

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True


    def setUp(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/flattenable.csv", schema= [('number', ta.int32),
                                                               ('abc', str),
                                                               ('food', str)], delimiter=',')

        print "create frame"
        self.frame = ta.Frame(csv)

    def test_lower_inclusive_strict_binning(self):
        self.frame.bin_column('number', [3, 5, 8], include_lowest=True, strict_binning=True)
        results = self.frame.take(100)
        for row in results:
            if row[0] < 3 or row[0] > 8:
                self.assertEqual(row[3], -1)
            if row[0] in [3, 4]:
                self.assertEqual(row[3], 0)
            if row[0] in [5, 6, 7, 8]:
                self.assertEqual(row[3], 1)

    def test_lower_inclusive_eager_binning(self):
        self.frame.bin_column('number', [3, 5, 8], include_lowest=True, strict_binning=False)
        results = self.frame.take(100)
        for row in results:
            if row[0] < 5:
                self.assertEqual(row[3], 0)
            if row[0] >= 5:
                self.assertEqual(row[3], 1)

    def test_upper_inclusive_strict_binning(self):
        self.frame.bin_column('number', [3, 5, 8], include_lowest=False, strict_binning=True)
        results = self.frame.take(100)
        for row in results:
            if row[0] < 3 or row[0] > 8:
                self.assertEqual(row[3], -1)
            if row[0] in [3, 4, 5]:
                self.assertEqual(row[3], 0)
            if row[0] in [6, 7, 8]:
                self.assertEqual(row[3], 1)

    def test_upper_inclusive_eager_binning(self):
        self.frame.bin_column('number', [3, 5, 8], include_lowest=False, strict_binning=False)
        results = self.frame.take(100)
        for row in results:
            if row[0] <= 5:
                self.assertEqual(row[3], 0)
            if row[0] > 5:
                self.assertEqual(row[3], 1)

    def test_bin_equal_width(self):
        cutoffs = self.frame.bin_column_equal_width('number', num_bins=3)
        self.assertEqual(cutoffs, [1, 4, 7, 10])
        results = self.frame.take(100)
        for row in results:
            if row[0] in [1, 2, 3]:
                self.assertEqual(row[3], 0)
            if row[0] in [4, 5, 6]:
                self.assertEqual(row[3], 1)
            if row[0] in [7, 8, 9, 10]:
                self.assertEqual(row[3], 2)

    def test_bin_equal_depth(self):
        cutoffs = self.frame.bin_column_equal_depth('number', num_bins=2)
        self.assertEqual(cutoffs, [1, 6, 10])
        results = self.frame.take(100)
        for row in results:
            if row[0] <= 5:
                self.assertEqual(row[3], 0)
            if row[0] > 5:
                self.assertEqual(row[3], 1)

    def test_default_num_bins(self):
        # the default number of bins is the square-root floored of the row count
        # number of bins is the cutoff length minus 1
        import math
        cutoffs = self.frame.bin_column_equal_width('number')
        self.assertEqual(len(cutoffs) - 1, math.floor(math.sqrt(self.frame.row_count)))

    def test_bin_with_missing_values(self):
        data = [[2],[3],[12],[15],[None],[17],[7],[1],[None]]
        schema = [('a', ta.int32)]
        frame_w_missings = ta.Frame(ta.UploadRows(data, schema))

        # Call bin column without specifying what to do with missings - this should ignore missings
        frame_w_missings.bin_column('a', [0,10,20], bin_column_name='default_bin')
        self.assertEqual([[0], [0], [1], [1], [-1], [1], [0], [0], [-1]], frame_w_missings.take(frame_w_missings.row_count, columns=['default_bin']))

        # Specify to ignore missings (should do the same thing as the default behavior)
        frame_w_missings.bin_column('a', [0,10,20], bin_column_name='ignore_missing', missing=ta.missing.ignore)
        self.assertEqual([[0], [0], [1], [1], [-1], [1], [0], [0], [-1]], frame_w_missings.take(frame_w_missings.row_count, columns=['ignore_missing']))

        # Treat missing values as 19
        frame_w_missings.bin_column('a', [0,10,20], bin_column_name='missing_is_19', missing=ta.missing.imm(19))
        self.assertEqual([[0], [0], [1], [1], [1], [1], [0], [0], [1]], frame_w_missings.take(frame_w_missings.row_count, columns=['missing_is_19']))

        frame_w_missings.bin_column('a', [0,10,20], bin_column_name='bin_missing_is_25', missing=ta.missing.imm(25))

if __name__ == "__main__":
    unittest.main()
