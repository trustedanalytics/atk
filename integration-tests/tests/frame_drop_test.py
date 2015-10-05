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

class FrameDropTest(unittest.TestCase):
    """
    Test frame drop()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_frame_drop(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/classification-compute.csv", schema= [('a', str),
                                                                          ('b', ta.int32),
                                                                          ('labels', ta.int32),
                                                                          ('predictions', ta.int32)], delimiter=',', skip_header_lines=1)

        print "create frame"
        frame = ta.Frame(csv, name="test_frame_drop")

        print "dropping frame by entity"
        ta.drop_frames(frame)
        frames = ta.get_frame_names()
        self.assertFalse("test_frame_drop" in frames, "test_frame_drop should not exist in list of frames")

        frame = ta.Frame(csv, name="test_frame_drop")

        print "dropping frame by name"
        ta.drop_frames("test_frame_drop")
        self.assertFalse("test_frame_drop" in frames, "test_frame_drop should not exist in list of frames")

    # Tests the generic ta.drop() using the frame proxy object
    def test_generic_drop_by_object(self):
        # drop existing frames
        for frame_name in ta.get_frame_names():
            ta.drop(frame_name)

        print "create frame"
        frame_name = "test_frame"
        frame = ta.Frame(name=frame_name)

        # Check that the frame we just created now exists
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frame names")

        print "drop frame"
        ta.drop(frame)

        # check that the frame no longer exists
        self.assertFalse(frame_name in ta.get_frame_names(), frame_name + " should not exist in the list of frames")

    # Tests the generic ta.drop() using the frame name
    def test_generic_drop_by_name(self):
        # drop existing frames
        for frame_name in ta.get_frame_names():
            ta.drop(frame_name)

        print "create frame"
        frame_name = "test_frame"
        frame = ta.Frame(name=frame_name)

        # Check that the frame we just created now exists
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frame names")

        print "drop frame"
        ta.drop(frame_name)

        # check that the frame no longer exists
        self.assertFalse(frame_name in ta.get_frame_names(), frame_name + " should not exist in the list of frames")

    # Tests the generic ta.drop() using a frame name that does not exist
    def test_generic_drop_by_invalid_name(self):
        import time
        frame_name = "frame_" + str(time.time())    # generate frame name with timestamp

        # Double check that we don't already have another frame, graph, or model with this name.  If we do, delete it.
        if (frame_name in ta.get_frame_names()):
            ta.drop_frame(frame_name)

        # Verify that calling drop on a frame that does not exist does not fail
        print "drop frame " + str(frame_name)
        ta.drop(frame_name)

    # Tests the generic drop() with an invalid object argument
    def test_generic_drop_with_invalid_object(self):
        # Setup test class, which won't be valid to drop()
        class InvalidDropObject:
            pass
        test_object = InvalidDropObject()
        # Call drop() with the test object
        print "call drop with invalid object type, and expect to get an AttributeError"
        with self.assertRaises(AttributeError):
            ta.drop(test_object)

if __name__ == "__main__":
    unittest.main()
