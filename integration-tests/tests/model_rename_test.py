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
import uuid     # for generating unique model names

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

    # Tests that we are able to rename a model
    def test_model_rename(self):
        model_name = str(uuid.uuid1()).replace('-','_')
        new_model_name = str(uuid.uuid1()).replace('-','_')

        model = ta.KMeansModel(name=model_name)
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should be in the list of models")

        model.name = new_model_name

        self.assertTrue(new_model_name in ta.get_model_names(), new_model_name + " should be in list of models")
        self.assertFalse(model_name in ta.get_model_names(), model_name + " shoule not be in list of models")

    # Tests that we cannot rename a model to have the same name as an existing model, graph, or frame
    def test_duplicate_model_rename(self):
        model_name1 = str(uuid.uuid1()).replace('-','_')
        model_name2 = str(uuid.uuid1()).replace('-','_')
        graph_name  = str(uuid.uuid1()).replace('-','_')
        frame_name  = str(uuid.uuid1()).replace('-','_')

        # Create models, graph, and frame to test with
        model1 = ta.KMeansModel(name=model_name1)
        model2 = ta.KMeansModel(name=model_name2)
        ta.Graph(name=graph_name)
        ta.Frame(name=frame_name)

        # After creating models, check that models with each name exists on the server
        self.assertTrue(model_name1 in ta.get_model_names(), model_name1 + " should exist in list of models")
        self.assertTrue(model_name2 in ta.get_model_names(), model_name2 + " should exist in list of models")

        # Try to rename model2 to have the same name as model1 (we expect an exception here)
        with self.assertRaises(Exception):
            model2.name = model_name1

        # Both model names should still exist on the server
        self.assertTrue(model_name1 in ta.get_model_names(), model_name1 + " should still exist in list of models")
        self.assertTrue(model_name2 in ta.get_model_names(), model_name2 + " should still exist in list of models")

        # Try to rename model1 to have the same name as the graph (we expect an exception here)
        with self.assertRaises(Exception):
            model1.name = graph_name

        # model1 and the graph should still exist on the server
        self.assertTrue(model_name1 in ta.get_model_names(), model_name1 + " should still exist in the list of models")
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should still exist in the list of graphs")

        # Try to rename model1 to have the same name as the frame (we expect an exception here)
        with self.assertRaises(Exception):
            model1.name = frame_name

        # model1 and the frame should still exist on the server
        self.assertTrue(model_name1 in ta.get_model_names(), model_name1 + " should still exist in the list of models")
        self.assertTrue(frame_name in ta.get_frames_names(), frame_name + " should still exist in the list of frames")


if __name__ == "__main__":
    unittest.main()
