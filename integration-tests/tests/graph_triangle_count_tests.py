# vim: set encoding=utf-8
#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
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

class GraphTriangleCountTest(unittest.TestCase):

    def test_triangle_count(self):
        graph_data = "/datasets/triangle_count_small.csv"
        schema = [('from_node',str),('to_node',str),('max_k',ta.int64),('cc',ta.int64)]
        frame = ta.Frame(ta.CsvFile(graph_data,schema))
        graph= ta.Graph()
        graph.define_vertex_type("node")
        graph.vertices["node"].add_vertices(frame,"from_node",["max_k","cc"])
        graph.vertices["node"].add_vertices(frame,"to_node",["max_k","cc"])
        graph.define_edge_type("edge","node","node",directed=True)
        graph.edges["edge"].add_edges(frame,"from_node","to_node")

        result = graph.graphx_triangle_count(output_property="triangle")

        frame_result = result['node']
        self.assertTrue(dict(frame_result.schema).has_key('triangle'))

if __name__ == "__main__":
    unittest.main()
