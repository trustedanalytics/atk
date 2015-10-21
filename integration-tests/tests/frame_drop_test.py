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
import uuid         # for generating unique frame names

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
        self.assertEqual(1, ta.drop_frames("test_frame_drop"), "drop_frames() should have deleted one frame")
        self.assertFalse("test_frame_drop" in frames, "test_frame_drop should not exist in list of frames")

    # Tests ta.drop_frames() with a frame name that does not exist
    def test_drop_frame_that_does_not_exist(self):
        frame_name = str(uuid.uuid1()).replace('-','_')

        self.assertFalse(frame_name in ta.get_frame_names(), frame_name + " should not exist in the list of frames")

        self.assertEqual(0, ta.drop_frames(frame_name), "drop_frames() should not have deleted any frames")

        # expect no exception

    # Tests drop_frames() and checks drop count with multiple of the same items
    def test_generic_drop_duplicate_items(self):
        frame_name = str(uuid.uuid1()).replace('-','_')
        frame = ta.Frame(name=frame_name)

        # Check that the frame we just created now exists
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frame names")

        # drop_frames() with multiple of the same item
        self.assertEqual(1, ta.drop_frames([frame, frame, frame_name]), "drop_frames() should have deleted 1 item")

        # Check that the frame no longer exists
        self.assertFalse(frame_name in ta.get_frame_names(), frame_name + " should not be in the list of frame names")

    # Tests the generic ta.drop() using the frame proxy object
    def test_generic_drop_by_object(self):
        frame_name = str(uuid.uuid1()).replace('-','_')

        frame = ta.Frame(name=frame_name)

        # Check that the frame we just created now exists
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frame names")

        self.assertEqual(1, ta.drop(frame), "drop() should have deleted one item")

        # Check that the frame no longer exists
        self.assertFalse(frame_name in ta.get_frame_names(), frame_name + " should not exist in the list of frames")

    # Tests the generic ta.drop() using the frame name
    def test_generic_drop_by_name(self):
        frame_name = str(uuid.uuid1()).replace('-','_')

        frame = ta.Frame(name=frame_name)

        # Check that the frame we just created now exists
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frame names")

        self.assertEqual(1, ta.drop(frame_name), "drop() should have deleted one item")

        # Check that the frame no longer exists
        self.assertFalse(frame_name in ta.get_frame_names(), frame_name + " should not exist in the list of frames")

    # Tests the generic ta.drop() using a frame name that does not exist
    def test_generic_drop_by_invalid_name(self):
        frame_name = str(uuid.uuid1()).replace('-','_')
        self.assertTrue(frame_name not in ta.get_frame_names(), frame_name + " should not exist in the list of frames")

        # Verify that calling drop on a frame that does not exist does not fail
        self.assertEqual(0, ta.drop(frame_name), "drop() with a non-existent item name should not have deleted items")

    # Tests the generic drop() with an invalid object argument
    def test_generic_drop_with_invalid_object(self):
        # Setup test class, which won't be valid to drop()
        class InvalidDropObject:
            pass
        test_object = InvalidDropObject()

        # Call drop() with the test object
        with self.assertRaises(AttributeError):
            ta.drop(test_object)

    # Tests the generic drop() with a list of frames/names
    def test_generic_drop_with_list(self):
        frame_name1 = str(uuid.uuid1()).replace('-','_')
        frame1 = ta.Frame(name=frame_name1)
        frame_name2 = str(uuid.uuid1()).replace('-','_')
        ta.Frame(name=frame_name2)

        # Create list with frame proxy object and frame name
        frameList = [frame1, frame_name2]

        # Check that the frames we just created now exist
        self.assertTrue(frame_name1 in ta.get_frame_names(), frame_name1 + " should exist in the list of frame names")
        self.assertTrue(frame_name2 in ta.get_frame_names(), frame_name2 + " should exist in the list of frame names")

        self.assertEqual(2, ta.drop(frameList), "drop() should have deleted the 2 items from the list")

        # Check that the frames no longer exist
        self.assertFalse(frame_name1 in ta.get_frame_names(), frame_name1 + " should not be in the list of frame names")
        self.assertFalse(frame_name2 in ta.get_frame_names(), frame_name2 + " should not be in the list of frame names")

    # Tests the generic drop() and checks drop count with multiple of the same items
    def test_generic_drop_duplicate_items(self):
        frame_name = str(uuid.uuid1()).replace('-','_')
        frame = ta.Frame(name=frame_name)

        # Check that the frame we just created now exists
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frame names")

        self.assertEqual(1, ta.drop(frame, frame, frame_name), "drop() should have deleted 1 item")

        # Check that the frame no longer exists
        self.assertFalse(frame_name in ta.get_frame_names(), frame_name + " should not be in the list of frame names")


if __name__ == "__main__":
    unittest.main()
