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

class GraphRenameTest(unittest.TestCase):
    """
    Tests graph renaming

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_graph_rename(self):
        graph_name = "original_graph_name"
        new_graph_name = "new_graph_name"

        # Create graph if it doesn't already exist
        if (graph_name not in ta.get_graph_names()):
            print "create graph"
            ta.Graph(name=graph_name)

        print "get graph named: " + str(graph_name)
        graph = ta.get_graph(graph_name)
        self.assertTrue(graph is not None)

        print "rename graph to: " + str(new_graph_name)
        graph.name = new_graph_name
        print "graphs: " + str(ta.get_graph_names())
        self.assertTrue(new_graph_name in ta.get_graph_names(), new_graph_name + " should be in list of graphs")
        self.assertFalse(graph_name in ta.get_graph_names(), graph_name + " shoule not be in list of graphs")

        # Drop graph to clean up after the test
        ta.drop_graphs(graph)


    # Tests that we cannot rename a graph to have the same name as an existing graph
    def test_duplicate_graph_rename(self):
        graph_name1 = "test_graph_name"
        graph_name2 = "other_graph_name"

        # Drop graphs by name, in case they already exist from a previous test
        ta.drop(graph_name1)
        ta.drop(graph_name2)

        print "create graph1 named: " + graph_name1
        graph1 = ta.Graph(name=graph_name1)

        print "create graph2 named: " + graph_name2
        graph2 = ta.Graph(name=graph_name2)

        # After creating graphs, check that graphs with each name exists on the server
        print "graphs: " + str(ta.get_graph_names())
        self.assertTrue(graph_name1 in ta.get_graph_names(), graph_name1 + " should exist in list of graphs")
        self.assertTrue(graph_name2 in ta.get_graph_names(), graph_name2 + " should exist in list of graphs")

        # Try to rename graph2 to have the same name as graph1 (we expect an exception here)
        print "check for an excpetion when we try to rename graph2 to have the same name as graph1"
        with self.assertRaises(Exception):
            graph2.name = graph_name1

        # Both graph names should still exist on the server
        print "graphs: " + str(ta.get_graph_names())
        self.assertTrue(graph_name1 in ta.get_graph_names(), graph_name1 + " should still exist in list of graphs")
        self.assertTrue(graph_name2 in ta.get_graph_names(), graph_name2 + " should still exist in list of graphs")

        # Delete both graphs from the server (to clean up after the test)
        ta.drop(graph1)
        ta.drop(graph2)

    # Tests that we cannot rename a graph to have the same name as an existing model
    def test_duplicate_model_rename(self):
        graph_name = "test_graph_name"
        model_name = "test_model_name"

        # Drop graph and model by name, in case they already exist from a previous test
        ta.drop(graph_name)
        ta.drop(model_name)

        print "create graph named: " + graph_name
        graph = ta.Graph(name=graph_name)
        self.assertTrue(graph.name == graph_name)
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should exist in the list of graphs")

        print "create model named: " + model_name
        model = ta.KMeansModel(name=model_name)
        self.assertTrue(model_name == model.name)
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should exist in the list of models")

        print "check for an exception when we try to rename the graph to the same name as the model"
        with self.assertRaises(Exception):
            graph.name = model_name

        # The original graph and model name should still exist on the server
        print "graphs: " + str(ta.get_graph_names())
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should still exist in the list of graphs")
        print "models: " + str(ta.get_model_names())
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should still exist in the list of models")

        # Delete the graph and the graph from the server (to clean up after the test)
        ta.drop(graph)
        ta.drop(model)

    # Tests that we cannot rename a graph to have the same name as an existing frame
    def test_duplicate_frame_rename(self):
        graph_name = "test_graph_name"
        frame_name = "test_frame_name"

        # Drop graph and frame by name, in case they already exist from a previous test
        ta.drop(graph_name)
        ta.drop(frame_name)

        print "create graph named: " + graph_name
        graph = ta.Graph(name=graph_name)
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should exist in the list of graphs")

        print "create frame named: " + frame_name
        frame = ta.Frame(name=frame_name)
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should exist in the list of frames")

        print "check for an exception when we try to rename the graph to the same name as the frame"
        with self.assertRaises(Exception):
            graph.name = frame_name

        # The original graph and frame name should still exist on the server
        print "graphs: " + str(ta.get_graph_names())
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should still exist in the list of graphs")
        print "frames: " + str(ta.get_frame_names())
        self.assertTrue(frame_name in ta.get_frame_names(), frame_name + " should still exist in the list of frames")

        # Delete the graph and the frame from the server (to clean up after the test)
        ta.drop(graph)
        ta.drop(frame)

if __name__ == "__main__":
    unittest.main()
