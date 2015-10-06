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

class GraphDropTest(unittest.TestCase):
    """
    Tests drop_graph() and drop() with graph names/objects.

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    # Tests ta.drop_graph() with the graph name
    def test_drop_graph_by_name(self):
        graph_name = "test_graph"

        # Create graph and verify that it's in the get_graph_names() list
        print "create graph"
        graph = ta.Graph(name=graph_name)
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should exist in the list of graphs")

        # Drop graph by name
        print "dropping graph by name"
        ta.drop_graphs(graph_name)
        self.assertFalse(graph_name in ta.get_graph_names(), graph_name + " should not exist in the list of graphs")

    # Tests ta.drop_graph() with the graph proxy object
    def test_drop_graph_by_object(self):
        graph_name = "test_graph"

        # Create graph and verify that it's in the get_graph_names() list
        print "create graph"
        graph = ta.Graph(name=graph_name)
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should exist in the list of graphs")

        # Drop graph using the graph object
        print "dropping graph by entity"
        ta.drop_graphs(graph)
        self.assertFalse(graph_name in ta.get_graph_names(), graph_name + " should not exist in the list of graphs")

    # Tests the generic ta.drop() using the graph proxy object
    def test_generic_drop_by_object(self):
        # drop existing graphs
        for graph_name in ta.get_graph_names():
            ta.drop(graph_name)

        print "create graph"
        graph_name = "test_graph"
        graph = ta.Graph(name=graph_name)

        # Check that the graph we just created now exists
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should exist in the list of graph names")

        print "drop graph"
        ta.drop(graph)

        # check that the graph no longer exists
        self.assertFalse(graph_name in ta.get_graph_names(), graph_name + " should not exist in the list of graph")

    # Tests the generic ta.drop() using the graph name
    def test_generic_drop_by_object(self):
        # drop existing graphs
        for graph_name in ta.get_graph_names():
            ta.drop(graph_name)

        print "create graph"
        graph_name = "test_graph"
        graph = ta.Graph(name=graph_name)

        # Check that the graph we just created now exists
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should exist in the list of graph names")

        print "drop graph"
        ta.drop(graph_name)

        # check that the graph no longer exists
        self.assertFalse(graph_name in ta.get_graph_names(), graph_name + " should not exist in the list of graph")

if __name__ == "__main__":
    unittest.main()
