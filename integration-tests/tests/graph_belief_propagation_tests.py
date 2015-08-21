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
#
# import unittest
# import trustedanalytics as atk
#
# # show full stack traces
# atk.errors.show_details = True
# atk.loggers.set_api()
# # TODO: port setup should move to a super class
# if atk.server.port != 19099:
#     atk.server.port = 19099
# atk.connect()()
#
# TODO: Commented out because of ClassNotFound error with spark local mode
# class GraphBeliefPropagationTest(unittest.TestCase):
#     def test_belief_propagation(self):
#         lbp_graphlab_input_data ="/datasets/lbp_graphlab_small.csv"
#         extra_vertex="/datasets/lbp_graphlab_append.csv"
#
#         schema = [("id1",atk.int32),("id2",atk.int32),("prior",str)]
#         schema2 = [("id1",atk.int32),("prior",str)]
#
#         lbp_frame = atk.Frame(atk.CsvFile(lbp_graphlab_input_data,schema))
#         extra_vertex_frame = atk.Frame(atk.CsvFile(extra_vertex,schema2))
#
#         graph = atk.Graph()
#         graph.define_vertex_type("nodes")
#         graph.vertices["nodes"].add_vertices(lbp_frame, "id1",["prior"])
#         graph.vertices["nodes"].add_vertices(extra_vertex_frame, "id1",["prior"])
#
#         graph.define_edge_type("edge","nodes","nodes",directed= False)
#         graph.edges["edge"].add_edges(lbp_frame,"id1","id2")
#
#         result = graph.ml.belief_propagation(prior_property="prior",posterior_property="lbp_output",max_iterations=1)
#
#         frame_result = result['vertex_dictionary']
#         self.assertTrue(dict(frame_result['nodes'].schema).has_key('lbp_output'))
#
# if __name__ == "__main__":
#     unittest.main()
