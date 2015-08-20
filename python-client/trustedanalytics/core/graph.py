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

titan_rule_deprecation = """
EdgeRule and VertexRule graph construction objects are deprecated.
Instead, construct a Graph object, then define and add vertices and
edges directly.  export_to_titan is available to obtain a TitanGraph.

Example:

>>> import trustedanalytics as ta
>>> g = ta.Graph()
>>> g.define_vertex_type('users')
>>> g.define_vertex_type('machines')
>>> g.vertices['users'].add_vertices(source_frame1, 'user')
>>> g.vertices['machines'].add_vertices(source_frame2, 'machine')
>>> g.define_edge_type('links', 'users', 'machines', directed=False)

>>> t = g.export_to_titan()
"""

__all__ = ["drop_frames", "drop_graphs", "EdgeRule", "Frame", "get_frame", "get_frame_names", "get_graph", "get_graph_names", "TitanGraph", "VertexRule"]

def _get_backend():
    from trustedanalytics.meta.config import get_graph_backend
    return get_graph_backend()


class RuleWithDifferentFramesError(ValueError):
    # TODO - Add docstring if this is really a |UDF|
    def __init__(self):
        ValueError.__init__(self, "Rule contains columns from different frames")


# TODO - make an Abstract Class
class Rule(object):
    """
    Graph rule base class.

    """
    # TODO - Docstrings

    def __init__(self):
        self.source_frame = self._validate()

    # A bunch of rule validation methods, each of which returns the common
    # source frame for the rule.  A little extra validation work here to enable
    # an easier API for the interactive user

    # Must be overridden:
    def _validate(self):
        """

        """
        # TODO - Docstrings
        raise NotImplementedError

    @staticmethod
    def _validate_source(source, frame):
        """
        Source: String or Column.

        """
        # TODO - Add examples
        if isinstance(source, Column):
            if frame is None:
                frame = source.frame
            elif frame != source.frame:
                raise RuleWithDifferentFramesError()
        elif not isinstance(source, basestring):
                raise TypeError("Rule contains invalid source type: " + type(source).__name__)
        return frame

    @staticmethod
    def _validate_property(key, value, frame):
        """

        """
        # TODO - Docstrings
        frame = Rule._validate_source(key, frame)
        frame = Rule._validate_source(value, frame)
        return frame

    @staticmethod
    def _validate_properties(properties):
        """

        """
        # TODO - Docstrings
        frame = None
        if properties:
            for k, v in properties.items():
                frame = Rule._validate_property(k, v, frame)
        return frame

    @staticmethod
    def _validate_same_frame(*frames):
        """
        Assures all non-None frames provided are in fact the same frame.

        """
        # TODO - Docstrings
        same = None
        for f in frames:
            if f:
                if same and f != same:
                    raise RuleWithDifferentFramesError()
                else:
                    same = f
        return same


class VertexRule(Rule):
    """
    Specifies a vertex and vertex properties.

    Dynamically pulling property names from a column can have a negative
    performance impact if there are many distinct values (hundreds of
    values are okay, thousands of values may take a long time).


    Parameters
    ----------
    id_key : string
        Static string or pulled from column source; the key for the uniquely
        identifying property for the vertex.
    id_value : Column source
        Vertex value.
        The unique value to identify this vertex.
    properties : dictionary
        {'vertex_type': ['L|R'], [property_name:property_value]}

        Vertex properties of the form property_name:property_value.
        The property_name (the key) is a string, and property_value is a
        literal value or a column source, which must be from the same Frame
        as the id_key and id_value arguments.


    Notes
    -----
    Vertex rules must include the property 'vertex_type':'L' for left-side, or
    'vertex_type':'R' for right-side, for the ALS and CGD (and other)
    algorithms to work properly.


    Examples
    --------
    .. only:: html

        .. code::

            >>> movie_vertex = ta.VertexRule('movie', my_frame['movie'], {'genre': my_frame['genre'], 'vertex_type':'L'})
            >>> user_vertex = ta.VertexRule('user', my_frame['user'], {'age': my_frame['age_1'], 'vertex_type':'R'})

    .. only:: latex

        .. code::

            >>> movie_vertex = ta.VertexRule('movie', my_frame['movie'],
            ... {'genre': my_frame['genre'], 'vertex_type':'L'})
            >>> user_vertex = ta.VertexRule('user', my_frame['user'],
            ... {'age': my_frame['age_1'], 'vertex_type':'R'})

    """
    def __init__(self, id_key, id_value, properties=None):
        #raise_deprecation_warning("VertexRule", titan_rule_deprecation)
        self.id_key = id_key
        self.id_value = id_value
        self.properties = properties or {}
        super(VertexRule, self).__init__()  # invokes validation

    def _as_json_obj(self):
        """JSON from point of view of Python API, NOT the REST API"""
        d = dict(self.__dict__)
        del d['source_frame']
        return d

    def __repr__(self):
        return json.dumps(self._as_json_obj())

    def _validate(self):
        """
        Checks that the rule has what it needs.

        Returns
        -------
        bool : ?
            # TODO - verify return type and give proper descriptions

        Examples
        --------

        .. code::

            >>> my_graph = ta.Graph(my_rule_a, my_rule_b, my_rule_1)
            >>> validation = my_graph.validate()

        """

        # TODO - Add docstring
        id_frame = self._validate_property(self.id_key, self.id_value, None)
        properties_frame = self._validate_properties(self.properties)
        return self._validate_same_frame(id_frame, properties_frame)


class EdgeRule(Rule):
    """
    Specifies an edge and edge properties.

    Dynamically pulling labels or property names from a column can
    have a negative performance impact if there are many distinct values
    (hundreds of values are okay, thousands of values may take a long time).


    Parameters
    ----------
    label : str or column source
        Edge label, can be constant string or pulled from column.
    tail : VertexRule
        Tail vertex ('from' vertex); must be from same Frame as head,
        label and any properties.
    head : VertexRule
        Head vertex ('to' vertex); must be from same Frame as tail,
        label and any properties.
    properties : dict
        Edge properties of the form property_name:property_value
        property_name is a string, and property_value is a literal value
        or a column source, which must be from same Frame as head,
        tail and label.
    bidirectional : bool (optional)
        Indicates the edge is bidirectional.
        Default is True.


    Examples
    --------
    .. only:: html

        .. code::

            >>> rating_edge = ta.EdgeRule('rating', movie_vertex, user_vertex, {'weight': my_frame['score']})

    .. only:: latex

        .. code::

            >>> rating_edge = ta.EdgeRule('rating', movie_vertex, user_vertex,
            ... {'weight': my_frame['score']})

    """
    def __init__(self, label, tail, head, properties=None, bidirectional=True, is_directed=None):
        #raise_deprecation_warning("EdgeRule", titan_rule_deprecation)
        self.bidirectional = bool(bidirectional)
        if is_directed is not None:
            raise_deprecation_warning("EdgeRule", "Parameter 'is_directed' is now called bidirectional' and has opposite polarity.")
            self.bidirectional = not is_directed

        self.label = label
        self.tail = tail
        self.head = head
        self.properties = properties or {}

        super(EdgeRule, self).__init__()  # invokes validation


    def _as_json_obj(self):
        """JSON from point of view of Python API, NOT the REST API"""
        d = dict(self.__dict__)
        d['tail'] = None if not self.tail else self.tail._as_json_obj()
        d['head'] = None if not self.head else self.head._as_json_obj()
        del d['source_frame']
        return d

    def __repr__(self):
        return json.dumps(self._as_json_obj())

    def _validate(self):
        """
        Checks that the rule has what it needs.

        Raises
        ------
        TypeError
            "Label argument must be a column or non-empty string"

        Returns
        -------
        bool
            # TODO - verify return type and give proper descriptions

        Examples
        --------

        .. code::

            Example

        """
        # TODO - Add docstring

        label_frame = None
        if isinstance(self.label, Column):
            label_frame = VertexRule('label', self.label)._validate()
        elif not self.label or not isinstance(self.label, basestring):
            raise TypeError("label argument must be a column or non-empty string")

        if isinstance(self.tail, VertexRule):
            tail_frame = self.tail._validate()
        else:
            raise TypeError("Invalid type %s for 'tail' argument. It must be a VertexRule." % self.tail)

        if isinstance(self.head, VertexRule):
            head_frame = self.head._validate()
        else:
            raise TypeError("Invalid type %s for 'head' argument. It must be a VertexRule." % self.head)
        properties_frame = self._validate_properties(self.properties)
        return self._validate_same_frame(label_frame, tail_frame, head_frame, properties_frame)


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
    @arg('rules', 'list of rule', """List of rules which specify how the graph will be created.
Default is an empty graph will be created.""")
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
            self.uri = self._backend.create(self, None, name, 'atk/frame', _info)

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
    Starting with a single source data frame, create a graph of 'user'
    and 'movie' vertices connected by 'rating' edges.

    Create a frame as the source for a graph:

    .. code::

        >>> print my_frame.schema
        [('user', ta.int32), ('vertex_type', str), ('movie', ta.int32), ('rating', str)]

    Define graph parsing rules:

    .. code::

        >>> user = ta.VertexRule("user", my_frame.user,
        ... {"vertex_type": my_frame.vertex_type})
        >>> movie = ta.VertexRule("movie", my_frame.movie)
        >>> rates = ta.EdgeRule("rating", user, movie,
        ... {"rating": my_frame.rating}, bidirectional = True)

    Create graph:

    .. code::

        >>> my_graph = ta.TitanGraph([user, movie, rates])

    |

    In another example, the vertex and edge rules can be sent to the method
    simultaneously.

    .. only:: html

        Define the rules:

        .. code::

            >>> srcips = ta.VertexRule("srcip", my_frame.srcip,{"vertex_type": "L"})
            >>> sports = ta.VertexRule("sport", my_frame.sport,{"vertex_type": "R"})
            >>> dstips = ta.VertexRule("dstip", my_frame.dstip,{"vertex_type": "R"})
            >>> dports = ta.VertexRule("dport", my_frame.dport,{"vertex_type": "L"})
            >>> from_edges = ta.EdgeRule("from_port", srcips, sports, {"fs_srcbyte": my_frame.fs_srcbyte,"tot_srcbyte": my_frame.tot_srcbyte, "fs_srcpkt": my_frame.fs_srcpkt},bidirectional=True)
            >>> to_edges = ta.EdgeRule("to_port", dstips, dports, {"fs_dstbyte": my_frame.fs_dstbyte,"tot_dstbyte": my_frame.tot_dstbyte, "fs_dstpkt": my_frame.fs_dstpkt},bidirectional=True)

     .. only:: latex

        Define the rules:

        .. code::

            >>> srcips = ta.VertexRule("srcip", my_frame.srcip,{"vertex_type": "L"})
            >>> sports = ta.VertexRule("sport", my_frame.sport,{"vertex_type": "R"})
            >>> dstips = ta.VertexRule("dstip", my_frame.dstip,{"vertex_type": "R"})
            >>> dports = ta.VertexRule("dport", my_frame.dport,{"vertex_type": "L"})
            >>> from_edges = ta.EdgeRule("from_port", srcips, sports,
            ... {"fs_srcbyte": my_frame.fs_srcbyte,
            ... "tot_srcbyte": my_frame.tot_srcbyte,
            ... "fs_srcpkt": my_frame.fs_srcpkt},bidirectional=True)
            >>> to_edges = ta.EdgeRule("to_port", dstips, dports,
            ... {"fs_dstbyte": my_frame.fs_dstbyte,
            ... "tot_dstbyte": my_frame.tot_dstbyte,
            ... "fs_dstpkt": my_frame.fs_dstpkt},bidirectional=True)

    Define the graph name:

    .. code::

        >>> gname = 'vast_netflow_topic_9'

    Create the graph:

    .. only:: html

        .. code::

            >>> my_graph = ta.TitanGraph([srcips, sports, from_edges, dstips, dports, to_edges], gname)

    .. only:: latex

        .. code::

            >>> my_graph = ta.TitanGraph([srcips, sports, from_edges,
            ... dstips, dports, to_edges], gname)

    """

    _entity_type = 'graph:titan'

    @api
    @arg('rules', 'list of rule', """List of rules which specify how the graph will be created.
Default is an empty graph will be created.""")
    @arg('name', str, """Name for the new graph.
Default is None.""")
    def __init__(self, rules=None, name=None, _info=None):
        """Initialize the graph."""
        try:
            self.uri = None
            if not hasattr(self, '_backend'):
                self._backend = _get_backend()
            _BaseGraph.__init__(self)
            self.uri = self._backend.create(self, rules, name, 'hbase/titan', _info)
            # logger.info('Created new graph "%s"', new_graph_name)
        except:
            raise IaError(logger)

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(TitanGraph,self).__repr__() + "(Unable to collect metadeta from server)"

    @api
    def __append(self, rules=None):
        """
        Append frame data to the current graph.

        Append updates existing edges and vertices or creates new ones if they
        do not exist.
        Vertices are considered the same if their id_key's and id_value's
        match.
        Edges are considered the same if they have the same source vertex,
        destination vertex, and label.


        Parameters
        ----------
        rules : list of rule
            List of rules which specify how the graph will be added to.
            Default is no data will be added.


        Examples
        --------
        This example shows appending new user and movie data to an existing
        graph.

        Create a frame as the source for additional data:

        .. only:: html

            .. code::

                >>> my_csv = ta.CsvFile("/movie.csv", schema = [('user', ta.int32), ('vertex_type', str), ('movie', ta.int32), ('rating', str)])

        .. only:: latex

            .. code::

                >>> my_csv = ta.CsvFile("/movie.csv", schema = [('user', ta.int32),
                ... ('vertex_type', str), ('movie', ta.int32), ('rating', str)])

            >>> my_frame = ta.Frame(csv)

        Define graph parsing rules:

        .. only:: html

            .. code::

                >>> user = ta.VertexRule("user", my_frame.user, {"vertex_type": my_frame.vertex_type})
                >>> movie = ta.VertexRule("movie", my_frame.movie)
                >>> rates = ta.EdgeRule("rating", user, movie, { "rating": my_frame.rating }, bidirectional = True)

        .. only:: latex

            .. code::

                >>> user = ta.VertexRule("user", my_frame.user,
                ... {"vertex_type": my_frame.vertex_type})
                >>> movie = ta.VertexRule("movie", my_frame.movie)
                >>> rates = ta.EdgeRule("rating", user, movie,
                ... { "rating": my_frame.rating }, bidirectional = True)

        Append data from the frame to an existing graph:

        .. code::

            >>> my_graph.append([user, movie, rates])

        |

        This example shows creating a graph from one frame and appending data
        to it from other frames.

        Create a frame as the source for a graph:

        .. only:: html

            .. code::

                >>> ratings_frame = ta.Frame(ta.CsvFile("/ratings.csv", schema = [('user_id', ta.int32), ('movie_id', ta.int32), ('rating', str)]))

        .. only:: latex

            .. code::

                >>> ratings_frame = ta.Frame(ta.CsvFile("/ratings.csv",
                ... schema = [('user_id', ta.int32), ('movie_id', ta.int32),
                ... ('rating', str)]))

        Define graph parsing rules:

        .. only:: html

            .. code::

                >>> user = ta.VertexRule("user", ratings_frame.user_id)
                >>> movie = ta.VertexRule("movie", ratings_frame.movie_id)
                >>> rates = ta.EdgeRule("rating", user, movie, { "rating": ratings_frame.rating }, bidirectional = True)

        .. only:: latex

            .. code::

                >>> user = ta.VertexRule("user", ratings_frame.user_id)
                >>> movie = ta.VertexRule("movie", ratings_frame.movie_id)
                >>> rates = ta.EdgeRule("rating", user, movie,
                ... { "rating": ratings_frame.rating }, bidirectional = True)

        Create graph:

        .. code::

            >>> my_graph = ta.Graph([user, movie, rates])

        Load additional properties onto the user vertices:

        .. only:: html

            .. code::

                >>> users_frame = ta.Frame(ta.CsvFile("/users.csv", schema = [('user_id', ta.int32), ('name', str), ('age', ta.int32)]))
                >>> user_additional = ta.VertexRule("user", users_frame.user_id, {"user_name": users_frame.name, "age": users_frame.age })
                >>> my_graph.append([user_additional])

        .. only:: latex

            .. code::

                >>> users_frame = ta.Frame(ta.CsvFile("/users.csv",
                ... schema = [('user_id', ta.int32), ('name', str),
                ... ('age', ta.int32)]))
                >>> user_additional = ta.VertexRule("user", users_frame.user_id,
                ... {"user_name": users_frame.name, "age": users_frame.age })
                >>> my_graph.append([user_additional])

        Load additional properties onto the movie vertices:

        .. only:: html

            .. code::

                >>> movie_frame = ta.Frame(ta.CsvFile("/movies.csv", schema = [('movie_id', ta.int32), ('title', str), ('year', ta.int32)]))
                >>> movie_additional = ta.VertexRule("movie", movie_frame.movie_id, {"title": movie_frame.title, "year": movie_frame.year })
                >>> my_graph.append([movie_additional])

        .. only:: latex

            .. code::

                >>> movie_frame = ta.Frame(ta.CsvFile("/movies.csv",
                ... schema = [('movie_id', ta.int32),
                ... ('title', str), ('year', ta.int32)]))
                >>> movie_additional = ta.VertexRule("movie",
                ... movie_frame.movie_id,
                ... {"title": movie_frame.title, "year": movie_frame.year })
                >>> my_graph.append([movie_additional])

        """
        self._backend.append(self, rules)

    def _get_new_graph_name(self):
        return "graph_" + uuid.uuid4().hex

    # TODO - consider:
    #def add(self, rules)
    #def remove(self, rules)
    #def add_props(self, rules)
    #def remove_props(self, rules)
