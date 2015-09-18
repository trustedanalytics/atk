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
        data = [[1, 'one', [1.0, 1.1]], [2, 'two', [2.0, 2.2]], [3, 'three', [3.0, 3.3]]]
        schema = [('n', int), ('s', str), ('v', ta.vector(2))]
        frame = ta.Frame(ta.UploadRows(data, schema))
        # print frame.inspect(wrap=10)
        taken = frame.take(5)
        self.assertEqual(len(data),len(taken))
        for r, row in enumerate(taken):
            self.assertEqual(len(data[r]),len(row))
            for c, column in enumerate(row):
                self.assertEqual(data[r][c], column)


if __name__ == "__main__":
    unittest.main()
