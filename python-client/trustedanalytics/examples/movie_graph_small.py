# vim: set encoding=utf-8

#
#  Copyright (c) 2015 Intel Corporation 
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

from trustedanalytics import examples

def run(path=r"datasets/movie_data_random.csv", ta=None):
    """
    Loads movie_data_random.csv into a frame, filters it by movie rating and creates a graph.
    We are not required to use movie_data_random.csv but rather it's schema. Any other csv file with the correct schema and delimeter will work.

    Parameters
    ----------
    path : str
        The HDFS path to the movie_data_random.csv dataset. If a path is not given the default is datasets/movie_data_random.csv. The dataset is
        available in the examples/datasets directory and in `github<https://github.com/trustedanalytics/atk/tree/master/python-client/trustedanalytics/examples/datasets>`__.
        Must be a valid HDFS path either fully qualified hdfs://some/path or relative the ATK rest servers HDFS home directory.

    ta : trusted analytics python import
        Can be safely ignored when running examples. It is only used during integration testing to pass pre-configured
        python client reference.


    Returns
    -------
        A dictionary with the frame and graph


    Datasets
    --------
      All the datasets can be found in the examples/datasets directory of the python client or in `github<https://github.com/trustedanalytics/atk/tree/master/python-client/trustedanalytics/examples/datasets>`__.


    Dataset
    -------
      Name : movie_data_random.csv

      schema:

        user_id(int32) , movie_id(int32) , rating(int32) , splits(str)

        sample

        .. code::
          1,-1,4,tr
          2,-1,1,tr
          3,-1,2,tr

      delimeter: ,


    Example
    -------
        To run the movie example first import the example.

        .. code::

          >>>import trustedanalytics.examples.movie_graph_small as movie

        After importing you can execute run method with the path to the dataset

        .. code::

          >>>movie.run("hdfs://FULL_HDFS_PATH")



    """
    FRAME_NAME = "MGS_frame"
    GRAPH_NAME = "MGS_graph"

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
    if FRAME_NAME in frames:
        print "Deleting old '{0}' frame.".format(FRAME_NAME)
        ta.drop_frames(FRAME_NAME)

    print "Building frame '{0}'.".format(FRAME_NAME)

    frame = ta.Frame(csv, FRAME_NAME)

    print "Inspecting frame '{0}'.".format(FRAME_NAME)

    print frame.inspect()

    print "Filter frame by rating."

    frame.filter(lambda row: row.rating >= 5)

    print frame.inspect()

    print "Creating graph '{0}'.".format(GRAPH_NAME)

    # Create a graph
    graphs = ta.get_graph_names()
    if GRAPH_NAME in graphs:
        print "Deleting old '{0}' graph.".format(GRAPH_NAME)
        ta.drop_graphs(GRAPH_NAME)


    graph = ta.Graph()
    graph.name = GRAPH_NAME
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
