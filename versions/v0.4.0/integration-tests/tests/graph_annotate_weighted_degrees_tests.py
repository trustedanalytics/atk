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

class GraphAnnotateWeightedDegreesTest(unittest.TestCase):
    def test_annotate_weighted_degrees(self):
        print "define csv file"
        schema_node = [("nodename", str),
                       ("in", ta.int64),
                       ("out", ta.int64),
                       ("undirectedcount", ta.int64),
                       ("isundirected", ta.int64),
                       ("outlabel", ta.int64),
                       ("insum", ta.float64),
                       ("outsum", ta.float64),
                       ("undirectedsum", ta.float64),
                       ("labelsum", ta.float64),
                       ("nolabelsum", ta.float64),
                       ("defaultsum", ta.float64),
                       ("integersum", ta.int64)]

        schema_directed = [("nodefrom", str),
                           ("nodeto", str),
                           ("value", ta.float64),
                           ("badvalue", str),
                           ("intvalue", ta.int32),
                           ("int64value", ta.int64)]

        schema_undirected = [("node1", str),
                             ("node2", str),
                             ("value", ta.float64)]

        schema_directed_label = [("nodefrom", str),
                                 ("nodeto", str),
                                 ("labeltest", ta.float64)]

        node_frame = ta.Frame(ta.CsvFile("/datasets/annotate_node_list.csv",schema_node))
        directed_frame = ta.Frame(ta.CsvFile("/datasets/annotate_directed_list.csv",schema_directed))
        undirected_frame = ta.Frame(ta.CsvFile("/datasets/annotate_undirected_list.csv", schema_undirected))
        directed_label_frame = ta.Frame(ta.CsvFile("/datasets/annotate_directed_label_list.csv", schema_directed_label))

        graph = ta.Graph()
        graph.define_vertex_type("primary")
        graph.vertices['primary'].add_vertices(node_frame,"nodename",["out",
                                                                      "undirectedcount",
                                                                      "isundirected",
                                                                      "outlabel",
                                                                      "in",
                                                                      "insum",
                                                                      "outsum",
                                                                      "undirectedsum",
                                                                      "labelsum",
                                                                      "nolabelsum",
                                                                      "defaultsum",
                                                                      "integersum"])
        graph.define_edge_type("directed","primary","primary",directed=True)
        graph.define_edge_type("labeldirected", "primary", "primary",
                               directed=True)
        graph.define_edge_type("undirected", "primary", "primary",
                               directed=False)

        graph.edges['directed'].add_edges(directed_frame, "nodefrom",
                                          "nodeto", ["value",
                                                     "badvalue",
                                                     "intvalue",
                                                     "int64value"])
        graph.edges['labeldirected'].add_edges(directed_label_frame,
                                               "nodefrom", "nodeto",
                                               ["labeltest"])
        graph.edges['undirected'].add_edges(undirected_frame, "node1",
                                            "node2", ["value"])
        output = graph.annotate_weighted_degrees("sumName", degree_option="in",edge_weight_property="value")
        self.assertTrue(type(output) is dict)
        self.assertTrue(output.has_key('primary'))
        frame_parquet = output['primary']
        self.assertTrue(dict(frame_parquet.schema).has_key('sumName'))

if __name__ == "__main__":
    unittest.main()
