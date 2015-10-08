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
import uuid     # for generating unique graph names

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class GraphRenameTest(unittest.TestCase):
    """
    Tests graph renaming

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    # Tests that we are able to rename a graph
    def test_graph_rename(self):
        graph_name = str(uuid.uuid1()).replace('-','_')
        new_graph_name = str(uuid.uuid1()).replace('-','_')

        # Create graph
        graph = ta.Graph(name=graph_name)
        graph.name = new_graph_name

        self.assertTrue(new_graph_name in ta.get_graph_names(), new_graph_name + " should be in list of graphs")
        self.assertFalse(graph_name in ta.get_graph_names(), graph_name + " should not be in list of graphs")

    # Tests that we cannot rename a graph to have the same name as an existing graph, frame, or model
    def test_duplicate_graph_rename(self):
        graph_name1 = str(uuid.uuid1()).replace('-','_')
        graph_name2 = str(uuid.uuid1()).replace('-','_')
        model_name =  str(uuid.uuid1()).replace('-','_')
        frame_name =  str(uuid.uuid1()).replace('-','_')

        # Create graphs, model, and frame
        graph1 = ta.Graph(name=graph_name1)
        graph2 = ta.Graph(name=graph_name2)
        ta.KMeansModel(name=model_name)
        ta.Frame(name=frame_name)

        # After creating graphs, check that graphs with each name exists on the server
        self.assertTrue(graph_name1 in ta.get_graph_names(), graph_name1 + " should exist in list of graphs")
        self.assertTrue(graph_name2 in ta.get_graph_names(), graph_name2 + " should exist in list of graphs")

        # Try to rename graph2 to have the same name as graph1 (we expect an exception here)
        with self.assertRaises(Exception):
            graph2.name = graph_name1

        # Both graph names should still exist on the server
        self.assertTrue(graph_name1 in ta.get_graph_names(), graph_name1 + " should still exist in list of graphs")
        self.assertTrue(graph_name2 in ta.get_graph_names(), graph_name2 + " should still exist in list of graphs")

        # Try to rename graph1 to have the same name as the frame (we expect an exception here)
        with self.assertRaises(Exception):
            graph1.name = frame_name

        # graph1 and the frame name should still exist on the server
        self.assertTrue(graph_name1 in ta.get_graph_names(), graph_name1 + " should still exist in the list of graphs")
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should still exist in the list of frames")

        # Try to rename graph1 to have the same name as the model (we expect an exception here)
        with self.assertRaises(Exception):
            graph1.name = model_name

        # graph1 and the frame name should still exist on the server
        self.assertTrue(graph_name1 in ta.get_graph_names(), graph_name1 + " should still exist in the list of graphs")
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should still exist in the list of models")

if __name__ == "__main__":
    unittest.main()
