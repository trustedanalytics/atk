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

class GraphSmokeTest(unittest.TestCase):
    """
    Smoke test basic graph operations to verify functionality that will be needed by all other tests.

    If these tests don't pass, there is no point in running other tests.

    This is a build-time test so it needs to be written to be as fast as possible:
        - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
        - Tests are ran in parallel
        - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_graph(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/movie.csv", schema= [('user', ta.int32),
                                            ('vertex_type', str),
                                            ('movie', ta.int32),
                                            ('rating', ta.int32),
                                            ('splits', str)])

        print "creating frame"
        frame = ta.Frame(csv)

        # TODO: add asserts verifying inspect is working
        print
        print frame.inspect(20)
        print
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        #self.assertEqual(frame.column_names, ['', '', '', '', ''])
        self.assertEquals(len(frame.column_names), 5, "frame should have 5 columns")

        print "create graph"
        graph = ta.Graph()

        self.assertIsNotNone(graph.uri)

        print "define vertices and edges"
        graph.define_vertex_type('movies')
        graph.define_vertex_type('users')
        graph.define_edge_type('ratings', 'users', 'movies', directed=True)
        self.assertEquals(graph.vertices['users'].row_count, 0, "making sure newly defined vertex frame does not have rows")
        self.assertEquals(graph.vertices['movies'].row_count, 0, "making sure newly defined vertex frame does not have rows")
        self.assertEquals(graph.edges['ratings'].row_count, 0, "making sure newly defined edge frame does not have rows")
        #self.assertEquals(graph.vertex_count, 0, "no vertices expected yet")
        #self.assertEquals(graph.edge_count, 0, "no edges expected yet")

        print "add_vertices() users"
        graph.vertices['users'].add_vertices( frame, 'user', [])

        # TODO: add asserts verifying inspect is working
        print
        print graph.vertices['users'].inspect(20)
        print
        self.assertEquals(graph.vertices['users'].row_count, 13)
        self.assertEquals(len(graph.vertices['users'].column_names), 3)
        #self.assertEquals(graph.vertices['users'].row_count, graph.vertex_count, "row count of user vertices should be same as vertex count on graph")

        print "add_vertices() movies"
        graph.vertices['movies'].add_vertices( frame, 'movie', [])
        self.assertEquals(graph.vertices['users'].row_count, 13)
        self.assertEquals(graph.vertices['movies'].row_count, 11)
        self.assertEquals(len(graph.vertices['users'].column_names), 3)
        self.assertEquals(len(graph.vertices['movies'].column_names), 3)
        #self.assertEquals(graph.vertex_count, 24, "vertex_count should be the total number of users and movies")

        print "add_edges()"
        graph.edges['ratings'].add_edges(frame, 'user', 'movie', ['rating'], create_missing_vertices=False)
        self.assertEquals(len(graph.edges['ratings'].column_names), 5)
        self.assertEquals(graph.edges['ratings'].row_count, 20, "expected 20 rating edges")

if __name__ == "__main__":
    unittest.main()
