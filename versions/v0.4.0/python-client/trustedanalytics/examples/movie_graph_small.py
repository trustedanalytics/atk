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
from trustedanalytics import examples

def run(path=r"datasets/movie_data_random.csv", ta=None):
    """
    The default home directory is hdfs://user/atkuser all the sample data sets are saved to
    hdfs://user/atkuser/datasets when installing through the rpm
    you will need to copy the data sets to hdfs manually otherwise and adjust the data set location path accordingly
    :param path: data set hdfs path can be full and relative path
    """
    NAME = "MGS"

    if ta is None:
        ta = examples.connect()
    #import trustedanalytics as ta

    #ta.connect()

    #csv schema definition
    schema = [("user_id", ta.int32),
              ("movie_id", ta.int32),
              ("rating", ta.int32),
              ("splits", str)]

    csv = ta.CsvFile(path, schema, skip_header_lines=1)

    frames = ta.get_frame_names()
    if NAME in frames:
        print "Deleting old '{0}' frame.".format(NAME)
        ta.drop_frames(NAME)
        
    print "Building frame '{0}'.".format(NAME)

    frame = ta.Frame(csv, NAME)

    print "Inspecting frame '{0}'.".format(NAME)

    print frame.inspect()

    print "Filter frame by rating."

    frame.filter(lambda row: row.rating >= 5)

    print frame.inspect()

    print "Creating graph '{0}'.".format(NAME)

    # Create a graph
    graphs = ta.get_graph_names()
    if NAME in graphs:
        print "Deleting old '{0}' graph.".format(NAME)
        ta.drop_graphs(NAME)


    graph = ta.Graph()
    graph.name = NAME
    # Create some rules
    graph.define_vertex_type("user_id")
    graph.define_vertex_type("movie_id")
    graph.define_edge_type("rating", "user_id", "movie_id", directed=True)

    #add data to graph
    graph.vertices["user_id"].add_vertices(frame, 'user_id')
    graph.vertices["movie_id"].add_vertices(frame, 'movie_id')
    graph.edges['rating'].add_edges(frame, 'user_id', 'movie_id', ['rating'])

    print graph.vertex_count
    print graph.edge_count
    print graph.vertices["user_id"].inspect(20)
    print graph.vertices["movie_id"].inspect(20)
    print graph.edges["rating"].inspect(20)


    return {"frame": frame, "graph": graph}