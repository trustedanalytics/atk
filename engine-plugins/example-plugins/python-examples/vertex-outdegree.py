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
# Vertex out-degree example - create a graph and count the vertex out-degree
#
# Depends on a movie.csv file
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal movie.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/movie.py')
#

import trustedanalytics as atk

# show full stack traces
atk.errors.show_details = True
#atk.loggers.set_api()
#ialoggers.set_http()

atk.connect()

print("server ping")
atk.server.ping()

print("define csv file")
csv = atk.CsvFile("/movie.csv", schema= [('user', atk.int32),
                                              ('vertexType', str),
                                              ('movie', atk.int32),
                                              ('rating', atk.int32),
                                              ('splits', str)])

print("create frame")
frame = atk.Frame(csv)

print("inspect frame")
frame.inspect(10)
print("frame row count " + str(frame.row_count))

print("create graph")
graph = atk.Graph()

print("created graph ")

print("define vertices and edges")
graph.define_vertex_type('movies')
graph.define_vertex_type('users')
graph.define_edge_type('ratings', 'users', 'movies', directed=False)

print "add user vertices"
graph.vertices['users'].add_vertices( frame, 'user')
# graph.vertex_count

graph.vertices['users'].inspect(20)

print "add movie vertices"
graph.vertices['movies'].add_vertices( frame, 'movie')
graph.edges['ratings'].add_edges(frame, 'user', 'movie', ['rating'], create_missing_vertices=False)

print ("vertex count: " + str(graph.vertex_count))
print ("edge count: " + str(graph.edge_count))

graph.edges['ratings'].inspect(20)

print "calculate vertex out-degree"
graph_outdegree = graph.vertex_outdegree()
graph_outdegree.inspect(20)

print "calculate vertex out-degree on titan graph"
titan_graph = graph.export_to_titan()
titan_outdegree = titan_graph.vertex_outdegree()
titan_outdegree.inspect(20)

print "done"
