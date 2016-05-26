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

from trustedanalytics.core.errorhandle import IaError

f, f2 = {}, {}

import logging
logger = logging.getLogger(__name__)
from trustedanalytics.core.decorators import *
api = get_api_decorator(logger)

from trustedanalytics.meta.metaprog import CommandInstallable as CommandLoadable
from trustedanalytics.meta.docstub import doc_stubs_import
from trustedanalytics.meta.namedobj import name_support
from trustedanalytics.core.files import OrientDBGraph
import uuid
import json

from trustedanalytics.core.column import Column

from trustedanalytics.meta.clientside import raise_deprecation_warning

__all__ = ["drop_frames", "drop_graphs", "Frame", "get_frame", "get_frame_names", "get_graph", "get_graph_names"]

def _get_backend():
    from trustedanalytics.meta.config import get_graph_backend
    return get_graph_backend()

# Graph
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from trustedanalytics.core.docstubs1 import _DocStubsGraph
    doc_stubs_import.success(logger, "_DocStubsGraph")
except Exception as e:
    doc_stubs_import.failure(logger, "_DocStubsGraph", e)
    class _DocStubsGraph(object): pass


@api
@name_support('graph')
class _BaseGraph(CommandLoadable):
    _entity_type = 'graph'
    def __init__(self):
        CommandLoadable.__init__(self)

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(_BaseGraph, self).__repr__() + " (Unable to collect metadata from server)"

    @api
    @property
    @returns(data_type=str, description="Status of the graph.")
    def __status(self):
        """
        Read-only property - Current graph life cycle status.

        One of three statuses: Active, Dropped, Finalized
        -   Active:    Entity is available for use
        -   Dropped:   Entity has been dropped by user or by garbage collection which found it stale
        -   Finalized: Entity's data has been deleted
        """
        try:
            return self._backend.get_status(self)
        except:
            return super(_BaseGraph, self).__repr__() + " (Unable to collect metadata from server)"

    @api
    @property
    @returns(data_type=str, description="Date string of the last time this frame's data was accessed")
    def __last_read_date(self):
        """
        Read-only property - Last time this frame's data was accessed.
        """
        try:
            return self._backend.get_last_read_date(self)
        except:
            return "(Unable to collect metadata from server)"


@api
class Graph(_DocStubsGraph, _BaseGraph):
    """Creates a seamless property graph.

A seamless graph is a collection of vertex and edge lists stored as frames.
This allows frame-like operations against graph data.
Many frame methods are available to work with vertices and edges.
Vertex and edge properties are stored as columns.

A seamless graph is better suited for bulk :term:`OLAP`-type operations
    """
    _entity_type = 'graph:'

    @api
    @arg('source', 'OrientDBGraph | None', "A source of initial data.")
    @arg('name', str, """Name for the new graph.
Default is None.""")
    def __init__(self, source=None, name=None, _info=None):
        """Initialize the graph.

    Examples
    --------
    This example uses a single source data frame and creates a graph of 'user'
    and 'movie' vertices connected by 'rating' edges.

    The first step is to bring in some data to create a frame as the source
    for a graph:

    <hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    -etc-

    </hide>
    >>> schema = [('viewer', str), ('profile', ta.int32), ('movie', str), ('rating', ta.int32)]
    >>> data1 = [['fred',0,'Croods',5],
    ...          ['fred',0,'Jurassic Park',5],
    ...          ['fred',0,'2001',2],
    ...          ['fred',0,'Ice Age',4],
    ...          ['wilma',0,'Jurassic Park',3],
    ...          ['wilma',0,'2001',5],
    ...          ['wilma',0,'Ice Age',4],
    ...          ['pebbles',1,'Croods',4],
    ...          ['pebbles',1,'Land Before Time',3],
    ...          ['pebbles',1,'Ice Age',5]]
    >>> data2 = [['betty',0,'Croods',5],
    ...          ['betty',0,'Jurassic Park',3],
    ...          ['betty',0,'Land Before Time',4],
    ...          ['betty',0,'Ice Age',3],
    ...          ['barney',0,'Croods',5],
    ...          ['barney',0,'Jurassic Park',5],
    ...          ['barney',0,'Land Before Time',3],
    ...          ['barney',0,'Ice Age',5],
    ...          ['bamm bamm',1,'Croods',5],
    ...          ['bamm bamm',1,'Land Before Time',3]]
    >>> frame = ta.Frame(ta.UploadRows(data1, schema))
    <progress>

    >>> frame2 = ta.Frame(ta.UploadRows(data2, schema))
    <progress>

    >>> frame.inspect()
    [#]  viewer   profile  movie             rating
    ===============================================
    [0]  fred           0  Croods                 5
    [1]  fred           0  Jurassic Park          5
    [2]  fred           0  2001                   2
    [3]  fred           0  Ice Age                4
    [4]  wilma          0  Jurassic Park          3
    [5]  wilma          0  2001                   5
    [6]  wilma          0  Ice Age                4
    [7]  pebbles        1  Croods                 4
    [8]  pebbles        1  Land Before Time       3
    [9]  pebbles        1  Ice Age                5


    Now, make an empty graph object:

    >>> graph = ta.Graph()

    Then, define the types of vertices and edges this graph will be made of:

    >>> graph.define_vertex_type('viewer')
    <progress>
    >>> graph.define_vertex_type('film')
    <progress>
    >>> graph.define_edge_type('rating', 'viewer', 'film')
    <progress>

    And finally, add the data to the graph:

    >>> graph.vertices['viewer'].add_vertices(frame, 'viewer', ['profile'])
    <progress>
    >>> graph.vertices['viewer'].inspect()
    [#]  _vid  _label  viewer   profile
    ===================================
    [0]     1  viewer  fred           0
    [1]     8  viewer  pebbles        1
    [2]     5  viewer  wilma          0

    >>> graph.vertices['film'].add_vertices(frame, 'movie')
    <progress>
    >>> graph.vertices['film'].inspect()
    [#]  _vid  _label  movie
    ===================================
    [0]    19  film    Land Before Time
    [1]    14  film    Ice Age
    [2]    12  film    Jurassic Park
    [3]    11  film    Croods
    [4]    13  film    2001

    >>> graph.edges['rating'].add_edges(frame, 'viewer', 'movie', ['rating'])
    <progress>
    >>> graph.edges['rating'].inspect()
    [#]  _eid  _src_vid  _dest_vid  _label  rating
    ==============================================
    [0]    24         1         14  rating       4
    [1]    22         1         12  rating       5
    [2]    21         1         11  rating       5
    [3]    23         1         13  rating       2
    [4]    29         8         19  rating       3
    [5]    30         8         14  rating       5
    [6]    28         8         11  rating       4
    [7]    27         5         14  rating       4
    [8]    25         5         12  rating       3
    [9]    26         5         13  rating       5

    Explore basic graph properties:

    >>> graph.vertex_count
    <progress>
    8

    >>> graph.vertices
    viewer : [viewer, profile], count = 3
    film : [movie], count = 5

    >>> graph.edge_count
    <progress>
    10

    >>> graph.edges
    rating : [rating], count = 10

    >>> graph.status
    u'ACTIVE'

    >>> graph.last_read_date
    <datetime.datetime>

    >>> graph
    Graph <unnamed>
    status = ACTIVE  (last_read_date = -etc-)
    vertices =
      viewer : [viewer, profile], count = 3
      film : [movie], count = 5
    edges =
      rating : [rating], count = 10

    Data from other frames can be added to the graph by making more calls
    to `add_vertices` and `add_edges`.

    <skip>
    >>> frame2 = ta.Frame(ta.CsvFile("/datasets/extra-movie-data.csv", frame.schema))
    <progress>

    </skip>
    >>> graph.vertices['viewer'].add_vertices(frame2, 'viewer', ['profile'])
    <progress>
    >>> graph.vertices['viewer'].inspect()
    [#]  _vid  _label  viewer     profile
    =====================================
    [0]     5  viewer  wilma            0
    [1]     1  viewer  fred             0
    [2]    31  viewer  betty            0
    [3]    35  viewer  barney           0
    [4]     8  viewer  pebbles          1
    [5]    39  viewer  bamm bamm        1

    >>> graph.vertices['film'].add_vertices(frame2, 'movie')
    <progress>
    >>> graph.vertices['film'].inspect()
    [#]  _vid  _label  movie
    ===================================
    [0]    13  film    2001
    [1]    14  film    Ice Age
    [2]    11  film    Croods
    [3]    19  film    Land Before Time
    [4]    12  film    Jurassic Park

    >>> graph.vertex_count
    <progress>
    11

    >>> graph.edges['rating'].add_edges(frame2, 'viewer', 'movie', ['rating'])
    <progress>

    >>> graph.edges['rating'].inspect(20)
    [##]  _eid  _src_vid  _dest_vid  _label  rating
    ===============================================
    [0]     24         1         14  rating       4
    [1]     22         1         12  rating       5
    [2]     21         1         11  rating       5
    [3]     23         1         13  rating       2
    [4]     29         8         19  rating       3
    [5]     30         8         14  rating       5
    [6]     28         8         11  rating       4
    [7]     27         5         14  rating       4
    [8]     25         5         12  rating       3
    [9]     26         5         13  rating       5
    [10]    60        39         19  rating       3
    [11]    59        39         11  rating       5
    [12]    53        31         19  rating       4
    [13]    54        31         14  rating       3
    [14]    52        31         12  rating       3
    [15]    51        31         11  rating       5
    [16]    57        35         19  rating       3
    [17]    58        35         14  rating       5
    [18]    56        35         12  rating       5
    [19]    55        35         11  rating       5

    >>> graph.edge_count
    <progress>
    20

    Now we'll copy the graph and then change it.

    >>> graph2 = graph.copy()
    <progress>

    >>> graph2
    Graph <unnamed>
    status = ACTIVE  (last_read_date = -etc-)
    vertices =
      viewer : [viewer, profile], count = 6
      film : [movie], count = 5
    edges =
      rating : [rating], count = 20

    We can rename the columns in the frames representing the vertices and edges,
    similar to regular frame operations.

    >>> graph2.vertices['viewer'].rename_columns({'viewer': 'person'})
    <progress>

    >>> graph2.vertices
    viewer : [person, profile], count = 6
    film : [movie], count = 5

    >>> graph2.edges['rating'].rename_columns({'rating': 'score'})
    <progress>

    >>> graph2.edges
    rating : [score], count = 20

    We can apply filter and drop functions to the vertex and edge frames.

    >>> graph2.vertices['viewer'].filter(lambda v: v.person.startswith("b"))
    <progress>

    >>> graph2.vertices['viewer'].inspect()
    [#]  _vid  _label  person     profile
    =====================================
    [0]    31  viewer  betty            0
    [1]    35  viewer  barney           0
    [2]    39  viewer  bamm bamm        1

    >>> graph2.vertices['viewer'].drop_duplicates("profile")
    <progress>

    <hide>
    >>> graph2.vertices['viewer'].sort('_vid')  # sort to ensure final inspect matches
    <progress>

    </hide>
    >>> graph2.vertices['viewer'].inspect()
    [#]  _vid  _label  person     profile
    =====================================
    [0]    31  viewer  betty            0
    [1]    39  viewer  bamm bamm        1

    Now check our edges to see that they have also be filtered.

    >>> graph2.edges['rating'].inspect()
    [#]  _eid  _src_vid  _dest_vid  _label  score
    =============================================
    [0]    60        39         19  rating      3
    [1]    59        39         11  rating      5
    [2]    53        31         19  rating      4
    [3]    54        31         14  rating      3
    [4]    52        31         12  rating      3
    [5]    51        31         11  rating      5

    Only source vertices 31 and 39 remain.

    Drop row for the movie 'Croods' (vid 41) from the film VertexFrame.

    >>> graph2.vertices['film'].inspect()
    [#]  _vid  _label  movie
    ===================================
    [0]    13  film    2001
    [1]    14  film    Ice Age
    [2]    11  film    Croods
    [3]    19  film    Land Before Time
    [4]    12  film    Jurassic Park

    >>> graph2.vertices['film'].drop_rows(lambda row: row.movie=='Croods')
    <progress>

    >>> graph2.vertices['film'].inspect()
    [#]  _vid  _label  movie
    ===================================
    [0]    13  film    2001
    [1]    14  film    Ice Age
    [2]    19  film    Land Before Time
    [3]    12  film    Jurassic Park

    Dangling edges (edges that correspond to the movie 'Croods', vid 41) were also removed:

    >>> graph2.edges['rating'].inspect()
    [#]  _eid  _src_vid  _dest_vid  _label  score
    =============================================
    [0]    52        31         12  rating      3
    [1]    54        31         14  rating      3
    [2]    60        39         19  rating      3
    [3]    53        31         19  rating      4


        """
        if not hasattr(self, '_backend'):
            self._backend = _get_backend()
        from trustedanalytics.rest.graph import GraphInfo
        if isinstance(_info, dict):
            _info = GraphInfo(_info)
        if isinstance(_info, GraphInfo):
            self.uri = _info.uri
        else:
            self.uri = self._backend.create(self, name, 'atk/frame', _info)
        if isinstance(source, OrientDBGraph):
            self._backend._import_orientdb(self,source)

        self._vertices = GraphFrameCollection(self, "vertices", self._get_vertex_frame, self._get_vertex_frames)
        self._edges = GraphFrameCollection(self, "edges", self._get_edge_frame, self._get_edge_frames)

        _BaseGraph.__init__(self)

    @api
    def ___get_vertex_frame(self, label):
        """
        return a VertexFrame for the associated label
        :param label: the label of the frame to return
        """
        return self._backend.get_vertex_frame(self.uri, label)

    @api
    def ___get_vertex_frames(self):
        """
        return all VertexFrames for this graph
        """
        return self._backend.get_vertex_frames(self.uri)

    @api
    def ___get_edge_frame(self, label):
        """
        return an EdgeFrame for the associated label
        :param label: the label of the frame to return
        """
        return self._backend.get_edge_frame(self.uri, label)

    @api
    def ___get_edge_frames(self):
        """
        return all EdgeFrames for this graph
        """
        return self._backend.get_edge_frames(self.uri)

    @api
    @property
    def __vertices(self):
        """
        Vertex frame collection

        Acts like a dictionary where the vertex type is the key, returning the particular VertexFrame

        Examples
        --------
        See :doc:`here <__init__>` for example usage in graph construction.
        """
        return self._vertices

    @api
    @property
    def __edges(self):
        """
        Edge frame collection

        Acts like a dictionary where the edge type is the key, returning the particular EdgeFrame

        Examples
        --------
        See :doc:`here <__init__>` for example usage in graph construction.
        """
        return self._edges

    @api
    @property
    def __vertex_count(self):
        """
        Get the total number of vertices in the graph.


        Returns
        -------
        int32
            The number of vertices in the graph.


        Examples
        --------
        See :doc:`here <__init__>` for example usage in graph construction.
        """
        return self._backend.get_vertex_count(self)

    @api
    @property
    @returns(int, 'Total number of edges in the graph')
    def __edge_count(self):
        """
        Get the total number of edges in the graph.


        Examples
        --------
        See :doc:`here <__init__>` for example usage in graph construction.
        """
        return self._backend.get_edge_count(self)


class GraphFrameCollection(object):
    """
    This class represents a collection of frames that make up either the edge
    or vertex types of a graph.
    """

    def __init__(self, graph, type_str, get_frame_func, get_frames_func):
        """
        :param get_frame_func: method to call to return a single frame in
            the collection
        :param get_frames_func: method to call to return all of the frames
            in the collection
        """
        self._graph = graph
        if type_str not in ["vertices", "edges"]:
            raise ValueError("Bad type_str %s in graph collection" % type_str)
        self._type_str = type_str
        self._get_frame_func = get_frame_func
        self._get_frames_func = get_frames_func

    def __getitem__(self, item):
        """
        Retrieve a single frame from the collection
        :param item:
        """
        return self._get_frame_func(item)

    def __iter__(self):
        """
        iterator for all of the frames in the collection. will call the server
        """
        for frame in self._get_frames_func():
            yield frame

    def __get_props_str(self, info):
        return "[%s]" % ", ".join(info['properties'])

    def __repr__(self):
        """
        printable representation of object
        """
        graph_info = self._graph._backend._get_graph_info(self._graph)
        if self._type_str == "vertices":
            return "\n".join(["%s : %s, count = %d" % (v['label'], self.__get_props_str(v), v['count']) for v in graph_info.vertices])
        if self._type_str == "edges":
            return "\n".join(["%s : %s, count = %d" % (e['label'], self.__get_props_str(e), e['count']) for e in graph_info.edges])
        return ""
