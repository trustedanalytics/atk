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

from trustedanalytics.core.errorhandle import IaError

f, f2 = {}, {}

import logging
logger = logging.getLogger(__name__)
from trustedanalytics.core.decorators import *
api = get_api_decorator(logger)

from trustedanalytics.meta.metaprog import CommandInstallable as CommandLoadable
from trustedanalytics.meta.docstub import doc_stubs_import
from trustedanalytics.meta.namedobj import name_support
import uuid
import json

from trustedanalytics.core.column import Column

from trustedanalytics.meta.clientside import raise_deprecation_warning

__all__ = ["drop_frames", "drop_graphs", "Frame", "get_frame", "get_frame_names", "get_graph", "get_graph_names", "TitanGraph"]

def _get_backend():
    from trustedanalytics.meta.config import get_graph_backend
    return get_graph_backend()

# _BaseGraph
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from trustedanalytics.core.docstubs1 import _DocStubsBaseGraph
    doc_stubs_import.success(logger, "_DocStubsBaseGraph")
except Exception as e:
    doc_stubs_import.failure(logger, "_DocStubsBaseGraph", e)
    class _DocStubsBaseGraph(object): pass


# TitanGraph
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from trustedanalytics.core.docstubs1 import _DocStubsTitanGraph
    doc_stubs_import.success(logger, "_DocStubsTitanGraph")
except Exception as e:
    doc_stubs_import.failure(logger, "_DocStubsTitanGraph", e)
    class _DocStubsTitanGraph(object): pass


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
class _BaseGraph(_DocStubsBaseGraph, CommandLoadable):
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
    @returns(data_type=str, description="Status of the graph")
    def __status(self):
        """
        Current graph life cycle status.

        One of three statuses: Active, Deleted, Deleted_Final
           Active:   available for use
           Deleted:  has been scheduled for deletion
           Deleted_Final: backend files have been removed from disk.
        """
        try:
            return self._backend.get_status(self)
        except:
            return super(_BaseGraph, self).__repr__() + " (Unable to collect metadata from server)"



@api
class Graph(_DocStubsGraph, _BaseGraph):
    """
    Creates a seamless property graph.

    A seamless graph is a collection of vertex and edge lists stored as frames.
    This allows frame-like operations against graph data.
    Many frame methods are available to work with vertices and edges.
    Vertex and edge properties are stored as columns.

    A seamless graph is better suited for bulk :term:`OLAP`-type operations
    whereas a Titan graph is better suited to :term:`OLTP`.


    Examples
    --------
    This example uses a single source data frame and creates a graph of 'user'
    and 'movie' vertices connected by 'rating' edges.

    The first step is to bring in some data to create a frame as the source
    for a graph:

    .. only:: html

        .. code::

            >>> my_schema = [('user_id', ta.int32), ('user_name', str), ('movie_id', ta.int32), ('movie_title', str), ('rating', str)]
            >>> my_csv = ta.CsvFile("/movie.csv", my_schema)
            >>> my_frame = ta.Frame(my_csv)

    .. only:: latex

        .. code::

            >>> my_schema = [('user_id', ta.int32), ('user_name', str),
            ... ('movie_id', ta.int32), ('movie_title', str), ('rating', str)]
            >>> my_csv = ta.CsvFile("/movie.csv", my_schema)
            >>> my_frame = ta.Frame(my_csv)

    Now, make an empty graph:

    .. code::

        >>> my_graph = ta.Graph()

    Then, define the types of vertices and edges this graph will be made of:

    .. code::

        >>> my_graph.define_vertex_type('users')
        >>> my_graph.define_vertex_type('movies')
        >>> my_graph.define_edge_type('ratings','users','movies',directed=True)

    And finally, add the data to the graph:

    .. only:: latex

        .. code::

            >>> my_graph.vertices['users'].add_vertices(my_frame, 'user_id', ['user_name'])
            >>> my_graph.vertices['movies'].add_vertices(my_frame, 'movie_id', ['movie_title'])
            >>> my_graph.edges['ratings'].add_edges(my_frame, 'user_id', 'movie_id', ['rating']

    .. only:: html

        .. code::

            >>> my_graph.vertices['users'].add_vertices(my_frame, 'user_id', ['user_name'])
            >>> my_graph.vertices['movies'].add_vertices(my_frame, 'movie_id', ['movie_title'])
            >>> my_graph.edges['ratings'].add_edges(my_frame, 'user_id',
            ... 'movie_id', ['rating'])

    |
    
    Adding additional data to the graph from another frame (my_frame2),
    is simply adding vertices (and edges) in row formation.

    .. code::

        >>> my_graph.vertices['users'].add_vertices(my_frame2, 'user_id', ['user_name'])

    Getting basic information about the graph:

    .. code::

        >>> my_graph.vertex_count
        >>> my_graph.edge_count
        >>> my_graph.vertices['users'].inspect(20)

    |

    This example uses multiple source data frames and creates a graph of
    'user' and 'movie' vertices connected by 'rating' edges.

    Create a frame as the source for a graph:

    .. code::

        >>> user_schema = [('user_id', ta.int32), ('user_name', str), ('age', ta.int32)]))
        >>> user_frame = ta.Frame(ta.CsvFile("/users.csv", userSchema)

        >>> movie_schema = [('movie_id', ta.int32), ('movie_title', str), ('year', str)]))
        >>> movie_frame = ta.Frame(ta.CsvFile("/movie.csv", movie_schema)

        >>> ratings_schema = [('ser_id', ta.int32), ('movie_id', ta.int32), ('rating', str)]))
        >>> ratings_frame = ta.Frame(ta.CsvFile("/ratings.csv", ratings_schema)

    Create a graph:

    .. code::

        >>> my_graph = ta.Graph()

    Define the types of vertices and edges this graph will be made of:

    .. code::

        >>> my_graph.define_vertex_type('users')
        >>> my_graph.define_vertex_type('movies')
        >>> my_graph.define_edge_type('ratings','users','movies',directed=True)

    Add data to the graph:

    .. only:: html

        .. code::

            >>> my_graph.vertices['users'].add_vertices(user_frame, 'user_id', ['user_name', 'age'])
            >>> my_graph.vertices['movies'].add_vertices(movie_frame, 'movie_id') # all columns automatically added as properties
            >>> my_graph.edges['ratings'].add_edges(ratings_frame, 'user_id', 'movie_id', ['rating'])

    .. only:: latex

        .. code::

            >>> my_graph.vertices['users'].add_vertices(user_frame, 'user_id',
            ... ['user_name', 'age'])
            >>> my_graph.vertices['movies'].add_vertices(movie_frame, 'movie_id')
            ... # all columns automatically added as properties
            >>> my_graph.edges['ratings'].add_edges(ratings_frame, 'user_id',
            ... 'movie_id', ['rating'])

    |

    This example shows edges between vertices of the same type.
    Specifically, "employees work under other employees".

    Create a frame to use as the source for the graph data:

    .. only:: html

        .. code::

            >>> employees_frame = ta.Frame(ta.CsvFile("employees.csv", schema = [('Employee', str), ('Manager', str), ('Title', str), ('Years', ta.int64)], skip_header_lines=1), 'employees_frame')

    .. only:: latex

        .. code::

            >>> employees_frame = ta.Frame(ta.CsvFile("employees.csv",
            ... schema = [('Employee', str), ('Manager', str),
            ... ('Title', str), ('Years', ta.int64)], skip_header_lines=1),
            ... 'employees_frame')

    Define a graph:

    .. code::

        >>> my_graph = ta.Graph()
        >>> my_graph.define_vertex_type('Employee')
        >>> my_graph.define_edge_type('worksunder', 'Employee', 'Employee', directed=True)

    Add data:

    .. only:: html

        .. code::

            >>> my_graph.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
            >>> my_graph.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'], create_missing_vertices = True)

    .. only:: latex

        .. code::

            >>> my_graph.vertices['Employee'].add_vertices(employees_frame,
            ... 'Employee', ['Title'])
            >>> my_graph.edges['worksunder'].add_edges(employees_frame,
            ... 'Employee', 'Manager', ['Years'],
            ... create_missing_vertices = True)

    Inspect the graph:

    .. code::

        >>> my_graph.vertex_count
        >>> my_graph.edge_count
        >>> my_graph.vertices['Employee'].inspect(20)
        >>> my_graph.edges['worksunder'].inspect(20)

    """
    _entity_type = 'graph:'

    @api
    @arg('name', str, """Name for the new graph.
Default is None.""")
    def __init__(self, name=None, _info=None):
        if not hasattr(self, '_backend'):
            self._backend = _get_backend()
        from trustedanalytics.rest.graph import GraphInfo
        if isinstance(_info, dict):
            _info = GraphInfo(_info)
        if isinstance(_info, GraphInfo):
            self.uri = _info.uri
        else:
            self.uri = self._backend.create(self, name, 'atk/frame', _info)

        self._vertices = GraphFrameCollection(self._get_vertex_frame, self._get_vertex_frames)
        self._edges = GraphFrameCollection(self._get_edge_frame, self._get_edge_frames)

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


        Examples
        --------
        Inspect vertices with the supplied label:

        .. code::

            >>> my_graph.vertices['label'].inspect()

        """
        return self._vertices

    @api
    @property
    def __edges(self):
        """
        Edge frame collection


        Examples
        --------
        Inspect edges with the supplied label:

        .. code::

            >>> my_graph.edges['label'].inspect()

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

        .. code::

            >>> my_graph.vertex_count

        The result given is:

        .. code::

            1194

        """
        return self._backend.get_vertex_count(self)

    @api
    @property
    @returns(int, 'Total number of edges in the graph')
    def __edge_count(self):
        """
        Get the total number of edges in the graph.


        Returns
        -------
        int32
            The number of edges in the graph.


        Examples
        --------
        .. code::

            >>> my_graph.edge_count

        The result given is:

        .. code::

            1194

        """
        return self._backend.get_edge_count(self)


class GraphFrameCollection(object):
    """
    This class represents a collection of frames that make up either the edge
    or vertex types of a graph.
    """

    def __init__(self, get_frame_func, get_frames_func):
        """
        :param get_frame_func: method to call to return a single frame in
            the collection
        :param get_frames_func: method to call to return all of the frames
            in the collection
        """
        self.get_frame_func = get_frame_func
        self.get_frames_func = get_frames_func

    def __getitem__(self, item):
        """
        Retrieve a single frame from the collection
        :param item:
        """
        return self.get_frame_func(item)

    def __iter__(self):
        """
        iterator for all of the frames in the collection. will call the server
        """
        for frame in self.get_frames_func():
            yield frame

    def __repr__(self):
        """
        printable representation of object
        """
        return repr(self.get_frames_func())


@api
class TitanGraph(_DocStubsTitanGraph, _BaseGraph):
    """
    Proxy to a graph in Titan, supports Gremlin query

    Examples
    --------
    Starting with a ta.Graph you can export to Titan to take advantage of Gremlin query.

    .. code::

        >>> graph = ta.get_graph("my_graph")
        >>> titan_graph = graph.export_to_titan("titan_graph")

    """

    _entity_type = 'graph:titan'

    @api
    def __init__(self, name=None, _info=None):
        """Initialize the graph."""
        try:
            self.uri = None
            if not hasattr(self, '_backend'):
                self._backend = _get_backend()
            _BaseGraph.__init__(self)
            self.uri = self._backend.create(self, name, 'hbase/titan', _info)
            # logger.info('Created new graph "%s"', new_graph_name)
        except:
            raise IaError(logger)

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(TitanGraph,self).__repr__() + "(Unable to collect metadeta from server)"

    def _get_new_graph_name(self):
        return "graph_" + uuid.uuid4().hex
