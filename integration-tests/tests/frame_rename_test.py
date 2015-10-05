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

class FrameRenameTest(unittest.TestCase):
    """
    Test frame rename()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_frame_rename(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/classification-compute.csv", schema= [('a', str),
                                                                          ('b', ta.int32),
                                                                          ('labels', ta.int32),
                                                                          ('predictions', ta.int32)], delimiter=',', skip_header_lines=1)

        print "create frame"
        frame = ta.Frame(csv, name="test_frame_rename")

        new_name = "test_frame_new_name"
        self.assertFalse(new_name in ta.get_frame_names(), "test_frame_new_name should not exist in list of frames")
        print "renaming frame"
        frame.name = new_name
        self.assertTrue(new_name in ta.get_frame_names(), "test_frame_new_name should exist in list of frames")

    def test_duplicate_frame_rename(self):
        frame_name1 = "test_frame_name"
        frame_name2 = "other_frame_name"
        print "define csv file"
        csv = ta.CsvFile("/datasets/classification-compute.csv", schema= [('a', str),
                                                                          ('b', ta.int32),
                                                                          ('labels', ta.int32),
                                                                          ('predictions', ta.int32)], delimiter=',', skip_header_lines=1)

        print "create frame1"
        frame1 = ta.Frame(csv, name=frame_name1)

        print "create frame2"
        frame2 = ta.Frame(csv, name=frame_name2)

        # After creating frames, check that frames with each name exists on the server
        self.assertTrue(frame_name1 in ta.get_frame_names(), frame_name1 + " should exist in list of frames")
        self.assertTrue(frame_name2 in ta.get_frame_names(), frame_name2 + " should exist in list of frames")

        # Try to rename frame2 to have the same name as frame1 (we expect an exception here)
        print "check for an excpetion when we try to rename frame2 to have the same name as frame1"
        with self.assertRaises(Exception):
            frame2.name = frame_name1

        # Both frame names should still exist on the server
        self.assertTrue(frame_name1 in ta.get_frame_names(), frame_name1 + " should exist in list of frames")
        self.assertTrue(frame_name2 in ta.get_frame_names(), frame_name2 + " should exist in list of frames")

        # Delete both frames from the server (to clean up after the test)
        ta.drop(frame_name1)
        ta.drop(frame_name2)
if __name__ == "__main__":
    unittest.main()
