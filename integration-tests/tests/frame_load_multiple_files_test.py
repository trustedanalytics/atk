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
atk.connect()

class FrameLoadMultipleFilesTest(unittest.TestCase):
    """
    Test loading a frame using wild cards to match multiple files

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_frame_loading_multiple_files_with_wildcard(self):
        csv = atk.CsvFile("/datasets/movie-part*.csv", schema= [('user', atk.int32),
                                                               ('vertex_type', str),
                                                               ('movie', atk.int32),
                                                               ('rating', atk.int32),
                                                               ('splits', str)])

        frame = atk.Frame(csv)
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        self.assertEquals(len(frame.column_names), 5, "frame should have 5 columns")
        self.assertGreaterEqual(frame._size_on_disk, 0, "frame size on disk should be non-negative")


if __name__ == "__main__":
    unittest.main()
