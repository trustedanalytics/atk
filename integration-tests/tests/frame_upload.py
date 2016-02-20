# vim: set encoding=utf-8
#  Copyright (c) 2016 Intel Corporation 
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


class FrameUploadTests(unittest.TestCase):
    """
    Test loading a frame with raw data uploaded from Python
    """
    _multiprocess_can_split_ = True

    def test_frame_upload_raw_list_data(self):
        """does round trip with list data --> upload to frame --> 'take' back to list and compare"""
        data = [[1, 'one', [1.0, 1.1]], [2, 'two', [2.0, 2.2]], [3, 'three', [3.0, 3.3]]]
        schema = [('n', int), ('s', str), ('v', ta.vector(2))]
        frame = ta.Frame(ta.UploadRows(data, schema))
        taken = frame.take(5)
        self.assertEqual(len(data),len(taken))
        for r, row in enumerate(taken):
            self.assertEqual(len(data[r]),len(row))
            for c, column in enumerate(row):
                self.assertEqual(data[r][c], column)

    def test_frame_upload_pandas(self):
        """does round trip pandas DF --> upload to frame --> download back to pandas and compare"""
        import pandas as pd
        from pandas.util.testing import assert_frame_equal
        import numpy as np
        data = [[1, 'one', [1.0, 1.1]], [2, 'two', [2.0, 2.2]], [3, 'three', [3.0, 3.3]]]
        schema = [('n', ta.int64), ('s', str), ('v', ta.vector(2))]  # 'n' is int64, pandas default
        source = dict(zip(zip(*schema)[0], zip(*data)))
        df0 = pd.DataFrame(source)
        self.assertEqual(np.int64, df0['n'].dtype)
        self.assertEqual(np.object, df0['s'].dtype)
        self.assertEqual(np.object, df0['v'].dtype)
        p = ta.Pandas(df0, schema)
        frame = ta.Frame(p)
        df1 = frame.download()
        # print repr(df0)
        # print repr(df1)
        assert_frame_equal(df0, df1)

if __name__ == "__main__":
    unittest.main()
