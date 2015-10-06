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

    # Tests that we cannot rename a frame to have the same name as an existing frame
    def test_duplicate_frame_rename(self):
        frame_name1 = "test_frame_name"
        frame_name2 = "other_frame_name"

        # Drop frames by name, in case they already exist from a previous test
        ta.drop(frame_name1)
        ta.drop(frame_name2)

        print "create frame1 named " + frame_name1
        frame1 = ta.Frame(name=frame_name1)
        self.assertTrue(frame1.name == frame_name1)

        print "create frame2 named " + frame_name2
        frame2 = ta.Frame(name=frame_name2)
        self.assertTrue(frame2.name == frame_name2)

        # After creating frames, check that frames with each name exists on the server
        self.assertTrue(frame_name1 in ta.get_frame_names(), frame_name1 + " should exist in list of frames")
        self.assertTrue(frame_name2 in ta.get_frame_names(), frame_name2 + " should exist in list of frames")

        # Try to rename frame2 to have the same name as frame1 (we expect an exception here)
        print "check for an excpetion when we try to rename frame2 to have the same name as frame1"
        with self.assertRaises(Exception):
            frame2.name = frame_name1

        # Both frame names should still exist on the server
        print "frames: " + str(ta.get_frames_names())
        self.assertTrue(frame_name1 in ta.get_frame_names(), frame_name1 + " should still exist in list of frames")
        self.assertTrue(frame_name2 in ta.get_frame_names(), frame_name2 + " should still exist in list of frames")

        # Delete both frames from the server (to clean up after the test)
        ta.drop(frame1)
        ta.drop(frame2)

    # Tests that we cannot rename a frame to have the same name as an existing graph
    def test_duplicate_graph_rename(self):
        frame_name = "test_frame_name"
        graph_name = "test_graph_name"

        # Drop frame and graph by name, in case they already exist from a previous test
        ta.drop(frame_name)
        ta.drop(graph_name)

        print "create frame named " + frame_name
        frame = ta.Frame(name=frame_name)
        self.assertTrue(frame.name == frame_name)
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frames")

        print "create graph named " + graph_name
        graph = ta.Graph(name=graph_name)
        self.assertTrue(graph.name == graph_name)
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should exist in the list of graphs")

        print "check for an exception when we try to rename the frame to the same name as the graph"
        with self.assertRaises(Exception):
            frame.name = graph_name

        # The original frame and graph name should still exist on the server
        print "frames: " + str(ta.get_frames_names())
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should still exist in the list of frames")
        print "graphs: " + str(ta.get_graph_names())
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should still exist in the list of graphs")

        # Delete the frame and the graph from the server (to clean up after the test)
        ta.drop(frame)
        ta.drop(graph)

    # Tests that we cannot rename a frame to have the same name as an existing model
    def test_duplicate_model_rename(self):
        frame_name = "test_frame_name"
        model_name = "test_model_name"

        # Drop frame and model by name, in case they already exist from a previous test
        ta.drop(frame_name)
        ta.drop(model_name)

        print "create frame named " + frame_name
        frame = ta.Frame(name=frame_name)
        self.assertTrue(frame.name == frame_name)
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frames")

        print "create model named " + model_name
        model = ta.KMeansModel(name=model_name)
        self.assertTrue(model.name == model_name)
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should exist in the list of models")

        print "check for an exception when we try to rename the frame to the same name as the model"
        with self.assertRaises(Exception):
            frame.name = model_name

        # The original frame and model name should still exist on the server
        print "frames: " + str(ta.get_frames_names())
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should still exist in the list of frames")
        print "models: " + str(ta.get_models_names())
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should still exist in the list of models")

        # Delete the frame and the model from the server (to clean up after the test)
        ta.drop(frame)
        ta.drop(model)

if __name__ == "__main__":
    unittest.main()
