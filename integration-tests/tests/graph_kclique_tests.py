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

class KcliqueTest(unittest.TestCase):
    def test_kclique(self):
        print "define csv file"
        noun_graph_data ="datasets/noun_graph_small.csv"
        schema = [("source",str),("target",str)]
        noun_words_frame = ta.Frame(ta.CsvFile(noun_graph_data,schema))
        graph = ta.Graph()

        graph.define_vertex_type("source")
        graph.vertices["source"].add_vertices(noun_words_frame,"source")
        graph.vertices["source"].add_vertices(noun_words_frame,"target")

        graph.define_edge_type("edge", "source", "source", False)
        graph.edges["edge"].add_edges(noun_words_frame,"source","target")

        output = graph.kclique_percolation(clique_size = 3, community_property_label = "community")
        output_dictionary = output['vertex_dictionary']

        self.assertTrue('source' in output_dictionary)

if __name__ == "__main__":
    unittest.main()
