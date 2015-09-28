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

class GraphPageRankTest(unittest.TestCase):
    def test_page_rank(self):
        """tests page_rank, +piggyback last_read_date testing"""
        graph_data = "/datasets/page_rank_test_data.csv"
        schema = [("followed", ta.int32),("follows",ta.int32)]
        frame = ta.Frame(ta.CsvFile(graph_data,schema))

        graph = ta.Graph()
        t0 = graph.last_read_date
        graph.define_vertex_type("node")
        graph.vertices["node"].add_vertices(frame,"follows")
        t1 = graph.last_read_date
        self.assertLess(t0, t1)  # make sure the last_read_date is updating

        graph.vertices["node"].add_vertices(frame,"followed")

        graph.define_edge_type("e1","node","node",directed=True)
        graph.edges["e1"].add_edges(frame,"follows","followed")
        t2 = graph.last_read_date
        self.assertLess(t1, t2)  # make sure the last_read_date is updating
        result = graph.graphx_pagerank(output_property="PageRank",max_iterations=2,convergence_tolerance=0.001)
        t3 = graph.last_read_date
        self.assertLess(t2, t3)  # make sure the last_read_date is updating

        vertex_dict = result['vertex_dictionary']
        edge_dict = result['edge_dictionary']

        self.assertTrue(dict(vertex_dict['node'].schema).has_key('PageRank'))

        self.assertTrue(dict(edge_dict['e1'].schema).has_key('PageRank'))

        t4 = graph.last_read_date
        self.assertEqual(t3, t4)  # metadata access should not have updated the date

if __name__ == "__main__":
  unittest.main()
