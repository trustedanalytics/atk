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
import trustedanalytics as atk

# show full stack traces
atk.errors.show_details = True
atk.loggers.set_api()
# TODO: port setup should move to a super class
if atk.server.port != 19099:
    atk.server.port = 19099
atk.connect()()

class GraphPageRankTest(unittest.TestCase):
    def test_page_rank(self):
        graph_data = "/datasets/page_rank_test_data.csv"
        schema = [("followed", atk.int32),("follows",atk.int32)]
        frame = atk.Frame(atk.CsvFile(graph_data,schema))

        graph = atk.Graph()
        graph.define_vertex_type("node")
        graph.vertices["node"].add_vertices(frame,"follows")
        graph.vertices["node"].add_vertices(frame,"followed")

        graph.define_edge_type("e1","node","node",directed=True)
        graph.edges["e1"].add_edges(frame,"follows","followed")

        result = graph.graphx_pagerank(output_property="PageRank",max_iterations=2,convergence_tolerance=0.001)

        vertex_dict = result['vertex_dictionary']
        edge_dict = result['edge_dictionary']

        self.assertTrue(dict(vertex_dict['node'].schema).has_key('PageRank'))

        self.assertTrue(dict(edge_dict['e1'].schema).has_key('PageRank'))

if __name__ == "__main__":
  unittest.main()
