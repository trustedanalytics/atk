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

class ModelRenameTest(unittest.TestCase):
    """
    Tests model renaming

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_model_rename(self):
        model_name = "original_model_name"
        new_model_name = "new_model_name"

        # Create model if it doesn't already exist
        if (model_name not in ta.get_model_names()):
            print "create model"
            ta.KMeansModel(name=model_name)

        print "get model named: " + str(model_name)
        model = ta.get_model(model_name)
        self.assertTrue(model is not None)

        print "rename model to: " + str(new_model_name)
        model.name = new_model_name
        self.assertTrue(new_model_name in ta.get_model_names(), new_model_name + " should be in list of models")
        self.assertFalse(model_name in ta.get_model_names(), model_name + " shoule not be in list of models")

        # Drop model to clean up after the test
        ta.drop_model(model)


    # Tests that we cannot rename a model to have the same name as an existing model
    def test_duplicate_model_rename(self):
        model_name1 = "test_model_name"
        model_name2 = "other_model_name"

        # Drop models by name, in case they already exist from a previous test
        ta.drop(model_name1)
        ta.drop(model_name2)

        print "create model1 named: " + model_name1
        model1 = ta.KMeansModel(name=model_name1)

        print "create model2 named: " + model_name2
        model2 = ta.KMeansModel(name=model_name2)

        # After creating models, check that models with each name exists on the server
        self.assertTrue(model_name1 in ta.get_model_names(), model_name1 + " should exist in list of models")
        self.assertTrue(model_name2 in ta.get_model_names(), model_name2 + " should exist in list of models")

        # Try to rename model2 to have the same name as model1 (we expect an exception here)
        print "check for an excpetion when we try to rename model2 to have the same name as model1"
        with self.assertRaises(Exception):
            model2.name = model_name1

        # Both model names should still exist on the server
        self.assertTrue(model_name1 in ta.get_model_names(), model_name1 + " should still exist in list of models")
        self.assertTrue(model_name2 in ta.get_model_names(), model_name2 + " should still exist in list of models")

        # Delete both models from the server (to clean up after the test)
        ta.drop(model_name1)
        ta.drop(model_name2)

    # Tests that we cannot rename a model to have the same name as an existing graph
    def test_duplicate_graph_rename(self):
        model_name = "test_model_name"
        graph_name = "test_graph_name"

        # Drop model and graph by name, in case they already exist from a previous test
        ta.drop(model_name)
        ta.drop(graph_name)

        print "create model named: " + model_name
        model = ta.KMeansModel(name=model_name)
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should exist in the list of models")

        print "create graph named: " + graph_name
        graph = ta.Graph(name=graph_name)
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should exist in the list of graphs")

        print "check for an exception when we try to rename the model to the same name as the graph"
        with self.assertRaises(Exception):
            model.name = graph_name

        # The original model and graph name should still exist on the server
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should still exist in the list of models")
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should still exist in the list of graphs")

        # Delete the model and the graph from the server (to clean up after the test)
        ta.drop(model)
        ta.drop(graph)

    # Tests that we cannot rename a model to have the same name as an existing frame
    def test_duplicate_frame_rename(self):
        model_name = "test_model_name"
        frame_name = "test_frame_name"

        # Drop model and frame by name, in case they already exist from a previous test
        ta.drop(model_name)
        ta.drop(frame_name)

        print "create model named: " + model_name
        model = ta.KMeansModel(name=model_name)
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should exist in the list of models")

        print "create frame named: " + frame_name
        frame = ta.Frame(name=frame_name)
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frames")

        print "check for an exception when we try to rename the model to the same name as the frame"
        with self.assertRaises(Exception):
            model.name = frame_name

        # The original model and frame name should still exist on the server
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should still exist in the list of models")
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should still exist in the list of frames")

        # Delete the model and the frame from the server (to clean up after the test)
        ta.drop(model)
        ta.drop(frame)

if __name__ == "__main__":
    unittest.main()
