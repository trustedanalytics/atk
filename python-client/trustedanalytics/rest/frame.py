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

"""
REST backend for frames
"""
import uuid
import logging
logger = logging.getLogger(__name__)
from collections import defaultdict, namedtuple, OrderedDict
import json
import sys
import trustedanalytics.rest.config as config

from trustedanalytics.core.frame import Frame
from trustedanalytics.core.atkpandas import Pandas
from trustedanalytics.core.column import Column
from trustedanalytics.core.files import CsvFile, LineFile, MultiLineFile, XmlFile, HiveQuery, HBaseTable, JdbcTable, UploadRows
from trustedanalytics.core.atktypes import *
from trustedanalytics.core.aggregation import agg
from trustedanalytics.core.ui import RowsInspection

from trustedanalytics.rest.atkserver import server
from trustedanalytics.rest.atktypes import get_data_type_from_rest_str, get_rest_str_from_data_type
from trustedanalytics.rest.command import CommandRequest, executor
from trustedanalytics.rest.spark import get_udf_arg, get_add_one_column_function, get_add_many_columns_function

TakeResult = namedtuple("TakeResult", ['data', 'schema'])
"""
Take result contains data and schema.
data contains only columns based on user specified columns
schema contains only columns baed on user specified columns
the data type under schema is also coverted to ATK types
"""


class FrameBackendRest(object):
    """REST plumbing for Frame"""

    def __init__(self, http_methods=None):
        self.server = http_methods or server

    def get_frame_by_uri(self, uri):
        logger.info("REST Backend: get_frame_by_uri")
        if uri is None:
            return None
        else:
            r = self.server.get(uri)
            payload = r.json()
            frame = Frame(_info=payload)
            return frame

    def create(self, frame, source, name, _info=None):
        logger.info("REST Backend: create frame with name %s" % name)
        if _info is not None and isinstance(_info, dict):
            _info = FrameInfo(_info)
        if isinstance(_info, FrameInfo):
            initialize_frame(frame, _info)
        elif isinstance(source, Frame):
            become_frame(frame, source.copy(name=name))
        elif isinstance(source, Column):
            become_frame(frame, source.frame.copy(columns=source.name, name=name))
        elif isinstance(source, list) and all(isinstance(iter, Column) for iter in source):
            become_frame(frame, source[0].frame.copy(columns=[s.name for s in source], name=name))
        else:
            payload = {'name': name }
            r = self.server.post('/frames', payload)
            logger.info("REST Backend: create frame response: " + r.text)
            frame_info = FrameInfo(r.json())
            initialize_frame(frame, frame_info)
            if source:
                try:
                    self.append(frame, source)
                except Exception:
                    self.server.delete(frame_info.uri)
                    raise
            return frame_info.name
        return frame.name

    def _create_new_frame(self, frame, name):
        """create helper method to call http and initialize frame with results"""
        payload = {'name': name }
        r = self.server.post('/frames', payload)
        logger.info("REST Backend: create frame response: " + r.text)
        frame_info = FrameInfo(r.json())
        initialize_frame(frame, frame_info)
        return frame_info.name

    def get_name(self, frame):
        return self._get_frame_info(frame).name

    def get_schema(self, frame):
        return self._get_frame_info(frame).schema

    def get_status(self, frame):
        return self._get_frame_info(frame).status

    def get_last_read_date(self, frame):
        return self._get_frame_info(frame).last_read_date

    def get_row_count(self, frame, where):
        if not where:
            return self._get_frame_info(frame).row_count
        # slightly faster generator to only return a list of one item, since we're just counting rows
        # TODO - there's got to be a better way to do this with the RDDs, trick is with Python.
        def icountwhere(predicate, iterable):
           return ("[1]" for item in iterable if predicate(item))
        arguments = {'frame': frame.uri,
                     'udf': get_udf_arg(frame, where, icountwhere)}
        return executor.execute("frame/count_where", self, arguments)

    def get_error_frame(self, frame):
        return self.get_frame_by_uri(self._get_frame_info(frame).error_frame_uri)

    def get_repr(self, frame):
        frame_info = self._get_frame_info(frame)
        try:
            frame_name = ('"%s"' % frame_info.name) if frame_info.name is not None else ' <unnamed>'
        except Exception as e:
            frame_name = "(Unable to determine name, error: %s)" % e
        try:
            schema = ', '.join(["%s:%s" % (name, data_type) for name, data_type in FrameSchema.from_types_to_strings(frame_info.schema)])
        except Exception as e:
            schema = "Unable to determine schema (%s)" % e
        try:
            row_count = frame_info.row_count
        except Exception as e:
            row_count = "Unable to determine row_count (%s)" % e

        try:
            status = frame_info.status
        except Exception as e:
            status = "Unable to determine status (%s)" % e

        try:
            last_read_date = frame_info.last_read_date.isoformat()
        except Exception as e:
            last_read_date = "Unable to determine last_read_date (%s)" % e

        if frame_info._has_vertex_schema():
            frame_type = "VertexFrame"
            graph_data = "\nLabel = %s" % frame_info.label
        elif frame_info._has_edge_schema():
            frame_type = "EdgeFrame"
            graph_data = "\nLabel = %s\nSource Vertex Label = %s\nDestination Vertex Label = %s\nDirected = %s" % (
                frame_info.label, frame_info.src_vertex_label, frame_info.dest_vertex_label, frame_info.directed)
        else:
            frame_type = "Frame"
            graph_data = ""
        return """{type} {name}{graph_data}
row_count = {row_count}
schema = [{schema}]
status = {status}  (last_read_date = {last_read_date})""".format(type=frame_type,
                                                                 name=frame_name,
                                                                 graph_data=graph_data,
                                                                 row_count=row_count,
                                                                 schema=schema,
                                                                 status=status,
                                                                 last_read_date=last_read_date)

    def _get_frame_info(self, frame):
        response = self.server.get(self._get_frame_full_uri(frame))
        return FrameInfo(response.json())

    def _get_frame_full_uri(self, frame):
        return self.server.create_full_uri(frame.uri)

    def _get_load_arguments(self, frame, source, data=None):
        if isinstance(source, CsvFile):
            return {'destination': frame.uri,
                    'source': {
                        "source_type": "file",
                        "uri": source.file_name,
                        "parser":{
                            "name": "builtin/line/separator",
                            "arguments": {
                                "separator": source.delimiter,
                                "skip_rows": source.skip_header_lines,
                                "schema":{
                                    "columns": source._schema_to_json()
                                }
                            }
                        },
                        "data": None
                    }
            }
        if isinstance( source, LineFile):
            return {'destination': frame.uri,
                    'source': {"source_type": "linefile",
                            "uri": source.file_name,
                            "parser": {"name": "invalid",
                                        "arguments":{"separator": ',',
                                                    "skip_rows": 0,
                                                    "schema": {"columns": [("data_lines",valid_data_types.to_string(str))]}
                                        }
                                },
                                "data": None
                                },
                    }

        if isinstance( source, XmlFile):
            return {'destination': frame.uri,
                    'source': {"source_type": "xmlfile",
                               "uri": source.file_name,
                               "start_tag":source.start_tag,
                               "end_tag":source.end_tag,
                               "data": None
                    },
                    }


        if isinstance( source, MultiLineFile):
            return {'destination': frame.uri,
                'source': {"source_type": "multilinefile",
                           "uri": source.file_name,
                           "start_tag":source.start_tag,
                           "end_tag":source.end_tag,
                           "data": None
                        },
                    }

        if isinstance(source, UploadRows):
            return{'destination': frame.uri,
                   'source': {"source_type": "strings",
                              "uri": "raw_list",
                              "parser": {"name": "builtin/upload",
                                         "arguments": { "separator": ',',
                                                       "skip_rows": 0,
                                                       "schema":{ "columns": source._schema_to_json()
                                    }
                                }
                              },
                              "data": data
                   }
            }
        if isinstance(source, Pandas):
            return{'destination': frame.uri,
                   'source': {"source_type": "strings",
                              "uri": "pandas",
                              "parser": {"name": "builtin/upload",
                                         "arguments": { "separator": ',',
                                                       "skip_rows": 0,
                                                       "schema":{ "columns": source._schema_to_json()
                                    }
                                }
                              },
                              "data": data
                   }
            }
        if isinstance(source, Frame):
            return {'source': { 'source_type': 'frame',
                                'uri': source.uri},
                    'destination': frame.uri}
        raise TypeError("Unsupported data source %s" % type(source))

    @staticmethod
    def _format_schema(schema):
        formatted_schema = []
        for name, data_type in schema:
            if not isinstance(name, basestring):
                raise ValueError("First value in schema tuple must be a string")
            formatted_data_type = valid_data_types.get_from_type(data_type)
            formatted_schema.append((name, formatted_data_type))
        return formatted_schema

    def add_columns(self, frame, expression, schema, columns_accessed=None):
        if not schema or not hasattr(schema, "__iter__"):
            raise ValueError("add_columns requires a non-empty schema of (name, type)")

        only_one_column = False
        if isinstance(schema[0], basestring):
            only_one_column = True
            schema = [schema]

        schema = self._format_schema(schema)
        names, data_types = zip(*schema)

        optimized_frame_schema = []
        if columns_accessed:
            if isinstance(columns_accessed, basestring):
                columns_accessed = [columns_accessed]
            frame_schema = frame.schema
            for i in columns_accessed:
                for j in frame_schema:
                    if i == j[0]:
                        optimized_frame_schema.append(j)

        # By default columns_accessed is an empty list and optimized frame schema is empty which implies frame.schema is considered to evaluate
        columns_accessed, optimized_frame_schema = ([], None) if columns_accessed is None else (columns_accessed, optimized_frame_schema)

        add_columns_function = get_add_one_column_function(expression, data_types[0]) if only_one_column \
            else get_add_many_columns_function(expression, data_types)
        from itertools import imap
        arguments = {'frame': frame.uri,
                     'column_names': names,
                     'column_types': [get_rest_str_from_data_type(t) for t in data_types],
                     'udf': get_udf_arg(frame, add_columns_function, imap, optimized_frame_schema),
                     'columns_accessed': columns_accessed}

        execute_update_frame_command('add_columns', arguments, frame)

    @staticmethod
    def _handle_error(result):
        if result and result.has_key("error_frame_uri"):
            sys.stderr.write("There were parse errors during load, please see frame.get_error_frame()\n")
            logger.warn("There were parse errors during load, please see frame.get_error_frame()")

    def append(self, frame, source):
        logger.info("REST Backend: append data to frame {0}: {1}".format(frame.uri, repr(source)))
        # for now, many data sources requires many calls to append
        if isinstance(source, list) or isinstance(source, tuple):
            for d in source:
                self.append(frame, d)
            return

        if isinstance(source, HBaseTable):
             arguments = source.to_json()
             arguments['destination'] = frame.uri
             result = execute_update_frame_command("frame/loadhbase", arguments, frame)
             self._handle_error(result)
             return

        if isinstance(source, JdbcTable):
             arguments = source.to_json()
             arguments['destination'] = frame.uri
             result = execute_update_frame_command("frame/loadjdbc", arguments, frame)
             self._handle_error(result)
             return

        if isinstance(source, HiveQuery):
             arguments = source.to_json()
             arguments['destination'] = frame.uri
             result = execute_update_frame_command("frame/loadhive", arguments, frame)
             self._handle_error(result)
             return

        if isinstance(source, Pandas):
            pan = source.pandas_frame
            if not source.row_index:
                pan = pan.reset_index()
            pan = pan.dropna(thresh=len(pan.columns))
            #number of columns should match the number of columns in the schema, else throw an error
            if len(pan.columns) != len(source.field_names):
                raise ValueError("Number of columns in Pandasframe {0} does not match the number of columns in the "
                                 " schema provided {1}.".format(len(pan.columns), len(source.field_names)))
            begin_index = 0
            iteration = 1
            end_index = config.upload_defaults.rows
            while True:
                pandas_rows = pan[begin_index:end_index].values.tolist()
                arguments = self._get_load_arguments(frame, source, pandas_rows)
                result = execute_update_frame_command("frame:/load", arguments, frame)
                self._handle_error(result)
                if end_index > len(pan.index):
                    break
                iteration += 1
                begin_index = end_index
                end_index = config.upload_defaults.rows * iteration
        elif isinstance(source, UploadRows):
            self._upload_raw_data_in_chunks(frame, source, source.data)
        else:
            arguments = self._get_load_arguments(frame, source)
            result = execute_update_frame_command("frame:/load", arguments, frame)
            self._handle_error(result)

    def _upload_raw_data_in_chunks(self, frame, source, data):
        """
        uploads data in chunks
        :param frame: frame proxy
        :param source: data source, like Pandas or RawListData
        :param data: list of data
        """
        # convoluted, but follows existing pattern
        begin_index = 0
        iteration = 1
        end_index = config.upload_defaults.rows
        while True:
            rows = data[begin_index:end_index]
            arguments = self._get_load_arguments(frame, source, rows)
            result = execute_update_frame_command("frame:/load", arguments, frame)
            self._handle_error(result)
            if end_index > len(data):
                break
            iteration += 1
            begin_index = end_index
            end_index = config.upload_defaults.rows * iteration

    def drop(self, frame, predicate):
        from trustedanalytics.rest.spark import ifilterfalse  # use the REST API filter, with a ifilterfalse iterator
        arguments = {'frame': frame.uri,
                     'udf': get_udf_arg(frame, predicate, ifilterfalse)}
        execute_update_frame_command("frame:/filter", arguments, frame)

    def filter(self, frame, predicate):
        from trustedanalytics.rest.spark import ifilter
        arguments = {'frame': frame.uri,
                     'udf': get_udf_arg(frame, predicate, ifilter)}
        execute_update_frame_command("frame:/filter", arguments, frame)

    def filter_vertices(self, frame, predicate, keep_matching_vertices = True):
        from trustedanalytics.rest.spark import ifilter
        from trustedanalytics.rest.spark import ifilterfalse

        if keep_matching_vertices:
            arguments = {'frame': frame.uri,
                         'udf': get_udf_arg(frame, predicate, ifilter)
                        }
        else:
            arguments = {'frame': frame.uri,
                         'udf': get_udf_arg(frame, predicate, ifilterfalse)
                        }
        execute_update_frame_command("frame:vertex/filter", arguments, frame)

    def column_statistic(self, frame, column_name, multiplier_column_name, operation):
        import numpy as np
        colTypes = dict(frame.schema)
        if not colTypes[column_name] in [np.float32, np.float64, np.int32, np.int64]:
            raise ValueError("unable to take statistics of non-numeric values")
        arguments = {'columnName': column_name, 'multiplierColumnName' : multiplier_column_name,
                     'operation' : operation}
        return execute_update_frame_command('columnStatistic', arguments, frame)

    def inspect(self, frame, n, offset, selected_columns, format_settings):
        # inspect is just a pretty-print of take, we'll do it on the client
        # side until there's a good reason not to
        result = self.take(frame, n, offset, selected_columns)
        data = result.data
        schema = result.schema
        return RowsInspection(data, schema, offset=offset, format_settings=format_settings)

    def join(self, left, right, left_on, right_on, how, name=None):
        if right_on is None:
            right_on = left_on
        arguments = {"name": name,
                     "how": how,
                     "left_frame": {"frame": left.uri, "join_column": left_on},
                     "right_frame": {"frame": right.uri, "join_column": right_on} }
        return execute_new_frame_command('frame:/join', arguments)

    def copy(self, frame, columns=None, where=None, name=None):
        if where:
            if not columns:
                column_names = frame.column_names
            elif isinstance(columns, basestring):
                column_names = [columns]
            elif isinstance(columns, dict):
                column_names = columns.keys()
            else:
                column_names = columns
            from trustedanalytics.rest.spark import get_udf_arg_for_copy_columns
            where = get_udf_arg_for_copy_columns(frame, where, column_names)
        else:
            where = None
        arguments = {'frame': frame.uri,
                     'columns': columns,
                     'where': where,
                     'name': name}
        return execute_new_frame_command('frame/copy', arguments)

    def group_by(self, frame, group_by_columns, aggregation):
        if group_by_columns is None:
            group_by_columns = []
        elif isinstance(group_by_columns, basestring):
            group_by_columns = [group_by_columns]

        first_column_name = None
        aggregation_list = []   #aggregationFunction : String, columnName : String, newColumnName

        for arg in aggregation:
            if arg == agg.count:
                if not first_column_name:
                    first_column_name = frame.column_names[0]  #only make this call once, since it goes to http - TODO, ultimately should be handled server-side
                aggregation_list.append({'function' :agg.count, 'column_name' : first_column_name, 'new_column_name' :"count"})
            elif isinstance(arg, dict):
                for k,v in arg.iteritems():
                    # leave the valid column check to the server
                    if isinstance(v, list) or isinstance(v, tuple):
                        for j in v:
                            if j not in agg:
                                raise ValueError("%s is not a valid aggregation function, like agg.max.  Supported agg methods: %s" % (j, agg))
                            aggregation_list.append({'function': j, 'column_name' : k, 'new_column_name' : "%s_%s" % (k, j)})
                    else:
                        aggregation_list.append({'function': v, 'column_name' :k, 'new_column_name' : "%s_%s" % (k, v)})
            else:
                raise TypeError("Bad type %s provided in aggregation arguments; expecting an aggregation function or a dictionary of column_name:[func]" % type(arg))

        arguments = {'frame': frame.uri,
                     'group_by_columns': group_by_columns,
                     'aggregations': aggregation_list}

        return execute_new_frame_command("group_by", arguments)


    def categorical_summary(self, frame, column_inputs):
        column_list_input = []
        for input in column_inputs:
            if isinstance(input, basestring):
                column_list_input.append({'column' : input})
            elif isinstance(input, tuple) and isinstance(input[0], basestring) and isinstance(input[1], dict):
                column_dict = {'column' : input[0]}
                column_dict.update(input[1])
                column_list_input.append(column_dict)
            else:
                raise TypeError('Column inputs should be specified as strings or 2-element Tuple consisting of column name as string and dictionary for additional parameters')

        arguments = {'frame': frame.uri,
                     'column_input': column_list_input}
        return executor.execute('frame/categorical_summary', self, arguments)


    def rename_columns(self, frame, column_names):
        if not isinstance(column_names, dict):
            raise ValueError("rename_columns requires a dictionary of string pairs")
        new_names = column_names.values()
        column_names = column_names.keys()

        arguments = {'frame': frame.uri, "original_names": column_names, "new_names": new_names}
        execute_update_frame_command('rename_columns', arguments, frame)

    def sort(self, frame, columns, ascending):
        if isinstance(columns, basestring):
            columns_and_ascending = [(columns, ascending)]
        elif isinstance(columns, list):
            if isinstance(columns[0], basestring):
                columns_and_ascending = map(lambda x: (x, ascending),columns)
            else:
                columns_and_ascending = columns
        else:
            raise ValueError("Bad type %s provided as argument; expecting basestring, list of basestring, or list of tuples" % type(columns))
        arguments = { 'frame': frame.uri, 'column_names_and_ascending': columns_and_ascending }
        execute_update_frame_command("sort", arguments, frame)

    def take(self, frame, n, offset, columns):
        def get_take_result():
            data = []
            schema = None
            while len(data) < n:
                url = '{0}/data?offset={2}&count={1}'.format(frame.uri,n + len(data), offset + len(data))
                result = executor.query(url)
                if not schema:
                    schema = result.schema
                if len(result.data) == 0:
                    break
                data.extend(result.data)
            return TakeResult(data, schema)
        if n < 0:
            raise ValueError("Count value needs to be positive. Provided %s" % n)

        if n == 0:
            return TakeResult([], frame.schema)
        result = get_take_result()

        schema = FrameSchema.from_strings_to_types(result.schema)

        if isinstance(columns, basestring):
            columns = [columns]

        updated_schema = schema
        if columns is not None:
            updated_schema = FrameSchema.get_schema_for_columns(schema, columns)
            indices = FrameSchema.get_indices_for_selected_columns(schema, columns)

        data = result.data
        if columns is not None:
            data = FrameData.extract_data_from_selected_columns(data, indices)

        return TakeResult(data, updated_schema)

    def create_vertex_frame(self, frame, source, label, graph, _info=None):
        logger.info("REST Backend: create vertex_frame with label %s" % label)
        if isinstance(_info, dict):
            _info = FrameInfo(_info)
        if isinstance(_info, FrameInfo):
            self.initialize_graph_frame(frame, _info, graph)
            return frame.name  # early exit here

        graph.define_vertex_type(label)
        source_frame = graph.vertices[label]
        self.copy_graph_frame(source_frame, frame)
        return source_frame.name

    def create_edge_frame(self, frame, label, graph, src_vertex_label, dest_vertex_label, directed, _info=None):
        logger.info("REST Backend: create vertex_frame with label %s" % label)
        if _info is not None and isinstance(_info, dict):
            _info = FrameInfo(_info)
        if isinstance(_info, FrameInfo):
            self.initialize_graph_frame(frame, _info, graph)
            return frame.name  # early exit here

        graph.define_edge_type(label, src_vertex_label, dest_vertex_label, directed)
        source_frame = graph.edges[label]
        self.copy_graph_frame(source_frame, frame)
        return source_frame.name

    def initialize_graph_frame(self, frame, frame_info, graph):
        """Initializes a frame according to given frame_info associated with a graph"""
        frame.uri = frame_info.uri
        frame._label = frame_info.label
        frame._graph = graph

    def copy_graph_frame(self, source, target):
        """Initializes a frame from another frame associated with a graph"""
        target.uri = source.uri
        target._label = source._label
        target._graph = source._graph



class FrameInfo(object):
    """
    JSON-based Server description of a Frame
    """
    def __init__(self, frame_json_payload):
        self._payload = frame_json_payload
        self._validate()

    def __repr__(self):
        return json.dumps(self._payload, indent=2, sort_keys=True)

    def __str__(self):
        return '%s "%s"' % (self.uri, self.name)
    
    def _validate(self):
        try:
            assert self.uri
        except KeyError:
            raise RuntimeError("Invalid response from server. Expected Frame info.")

    @property
    def name(self):
        return self._payload.get('name', None)
    
    @property
    def uri(self):
        return self._payload['uri']

    @property
    def schema(self):
        return FrameSchema.from_strings_to_types(self._payload['schema']['columns'])

    @property
    def row_count(self):
        try:
            return int(self._payload['row_count'])
        except KeyError:
            return 0

    @property
    def error_frame_uri(self):
        return self._payload.get('error_frame_uri', None)

    @property
    def label(self):
        try:
            schema = self._payload['schema']
            if self._has_vertex_schema():
                return schema['vertex_schema']['label']
            if self._has_edge_schema():
                return schema['edge_schema']['label']
            return None
        except:
            return None

    @property
    def directed(self):
        if not self._has_edge_schema():
            return None
        try:
            return self._payload['schema']['edge_schema']['directed']
        except:
            return None

    @property
    def dest_vertex_label(self):
        if not self._has_edge_schema():
            return None
        try:
            return self._payload['schema']['edge_schema']['dest_vertex_label']
        except:
            return None

    @property
    def src_vertex_label(self):
        if not self._has_edge_schema():
            return None
        try:
            return self._payload['schema']['edge_schema']['src_vertex_label']
        except:
            return None

    @property
    def status(self):
        return self._payload['status']

    @property
    def last_read_date(self):
        return valid_data_types.datetime_from_iso(self._payload['last_read_date'])

    def _has_edge_schema(self):
        return "edge_schema" in self._payload['schema']

    def _has_vertex_schema(self):
        return "vertex_schema" in self._payload['schema']

    def update(self, payload):
        if self._payload and self.uri != payload['uri']:
            msg = "Invalid payload, frame URI mismatch %s when expecting %s" \
                  % (payload['uri'], self.uri)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload = payload


class FrameSchema:
    """
    Conversion functions for schema representations

    The standard schema representation is
    1. A list of (name, data_type) tuples of the form (str, type) [types]

    Other representations include:
    2.  A list tuples of the form (str, str)  --used in JSON      [strings]
    3.  A list of BigColumn objects                               [columns]
    4.  An OrderedDict of name: data_type of the form str: type   [dict]
    """

    @staticmethod
    def from_types_to_strings(s):
        return [(name, get_rest_str_from_data_type(data_type)) for name, data_type in s]

    @staticmethod
    def from_strings_to_types(s):
        return [(column['name'], get_data_type_from_rest_str(column['data_type'])) for column in s]

    @staticmethod
    def get_schema_for_columns(schema, selected_columns):
        indices = FrameSchema.get_indices_for_selected_columns(schema, selected_columns)
        return [schema[i] for i in indices]

    @staticmethod
    def get_indices_for_selected_columns(schema, selected_columns):
        indices = []
        schema_columns = [col[0] for col in schema]
        if not selected_columns:
            raise ValueError("Column list must not be empty. Please choose from : (%s)" % ",".join(schema_columns))
        for column in selected_columns:
            try:
                indices.append(schema_columns.index(column))
            except:
                raise ValueError("Invalid column name %s provided"
                                 ", please choose from: (%s)" % (column, ",".join(schema_columns)))

        return indices


class FrameData:

    @staticmethod
    def extract_data_from_selected_columns(data_in_page, indices):
        new_data = []
        for row in data_in_page:
            new_data.append([row[index] for index in indices])

        return new_data

def initialize_frame(frame, frame_info):
    """Initializes a frame according to given frame_info"""
    frame.uri = frame_info.uri

def become_frame(frame, source_frame):
    """Initializes a frame proxy according to another frame proxy"""
    frame.uri = source_frame.uri

def execute_update_frame_command(command_name, arguments, frame):
    """Executes command and updates frame with server response"""
    #support for non-plugin methods that may not supply the full name
    if not command_name.startswith('frame'):
        command_name = 'frame/' + command_name
    command_request = CommandRequest(command_name, arguments)
    command_info = executor.issue(command_request)
    if command_info.result.has_key('schema'):
        initialize_frame(frame, FrameInfo(command_info.result))
        return None
    if (command_info.result.has_key('value') and len(command_info.result) == 1):
        return command_info.result.get('value')
    return command_info.result


def execute_new_frame_command(command_name, arguments):
    """Executes command and creates a new Frame object from server response"""
    #support for non-plugin methods that may not supply the full name
    if not command_name.startswith('frame'):
        command_name = 'frame/' + command_name
    command_request = CommandRequest(command_name, arguments)
    command_info = executor.issue(command_request)
    frame_info = FrameInfo(command_info.result)
    return Frame(_info=frame_info)
