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

"""
REST backend for graphs
"""
import json
import logging
import uuid

logger = logging.getLogger(__name__)

from trustedanalytics.rest.atkserver import server
from trustedanalytics.core.frame import VertexFrame, EdgeFrame
from trustedanalytics.rest.command import executor
from trustedanalytics.meta.names import indent
from trustedanalytics import valid_data_types


def initialize_graph(graph, graph_info):
    """Initializes a graph according to given graph_info"""
    graph.uri = graph_info.uri
    graph._name = graph_info.name
    return graph


class GraphBackendRest(object):

    def __init__(self, http_methods = None):
        self.server = http_methods or server

    def create(self, graph, name, storage_format, _info=None):
        logger.info("REST Backend: create graph with name %s: " % name)
        if isinstance(_info, dict):
            _info = GraphInfo(_info)
        if isinstance(_info, GraphInfo):
            return initialize_graph(graph,_info).uri # Early exit here
        new_graph_id = self._create_new_graph(graph, name, storage_format, True if name else False)
        return new_graph_id

    def _create_new_graph(self, graph, name, storage_format, is_named):
        payload = {'name': name, 'storage_format': storage_format, 'is_named': is_named}
        r=self.server.post('graphs', payload)
        logger.info("REST Backend: create graph response: " + r.text)
        graph_info = GraphInfo(r.json())
        initialize_graph(graph, graph_info)
        return graph_info.uri
    
    def _get_new_graph_name(self,source=None):
        try:
            annotation ="_" + source.annotation
        except:
            annotation= ''
        return "graph_" + uuid.uuid4().hex + annotation

    def get_repr(self, graph):
        graph_info = self._get_graph_info(graph)
        graph_name = '"%s"' % graph_info.name if graph_info.name is not None else '<unnamed>'
        r = ['%s %s' % (graph.__class__.__name__, graph_name),
             'status = %s  (last_read_date = %s)' % (graph_info.status,
                                                     graph_info.last_read_date.isoformat())]

        if hasattr(graph, 'vertices'):
            r.extend(['vertices = ', indent(repr(graph.vertices) or "(None)", 2)])
        if hasattr(graph, 'edges'):
            r.extend(['edges = ', indent(repr(graph.edges) or "(None)", 2)])
        return "\n".join(r)

    def get_status(self, graph):
        graph_info = self._get_graph_info(graph)
        return graph_info.status

    def get_last_read_date(self, graph):
        return self._get_graph_info(graph).last_read_date

    def _get_graph_info(self, graph):
        response = self.server.get(self._get_graph_full_uri(graph))
        return GraphInfo(response.json())

    def _get_graph_full_uri(self,graph):
        return self.server.create_full_uri(graph.uri)

    def _import_orientdb(self, graph, source):
        arguments = source.to_json()
        arguments['graph'] = graph.uri
        return executor.execute("graph:/_import_orientdb", graph, arguments)

    def get_vertex_frames(self, graph_uri):
        r = self.server.get('%s/vertices' % graph_uri)
        return [VertexFrame(_info=x) for x in r.json()]

    def get_vertex_frame(self, graph_uri, label):
        r = self.server.get('%s/vertices?label=%s' % (graph_uri, label))
        return VertexFrame(_info=r.json())

    def get_edge_frames(self, graph_uri):
        r = self.server.get('%s/edges' % graph_uri)
        return [EdgeFrame(_info=x) for x in r.json()]

    def get_edge_frame(self,graph_uri, label):
        r = self.server.get('%s/edges?label=%s' % (graph_uri, label))
        return EdgeFrame(_info=r.json())

    def get_vertex_count(self, graph):
        arguments = {'graph': graph.uri}
        return executor.execute("graph:/vertex_count", graph, arguments)['value']

    def get_edge_count(self, graph):
        arguments = {'graph': graph.uri}
        return executor.execute("graph:/edge_count", graph, arguments)['value']

class GraphInfo(object):
    """
    JSON based Server description of a Graph
    """
    def __init__(self, graph_json_payload):
        self._payload = graph_json_payload

    def __repr__(self):
        return json.dumps(self._payload, indent =2, sort_keys=True)

    def __str__(self):
        return '%s "%s"' % (self.uri, self.name)

    @property
    def name(self):
        return self._payload.get('name', None)

    @property
    def entity_type(self):
        return self._payload['entity_type']

    @property
    def uri(self):
        return self._payload['uri']

    @property
    def links(self):
        return self._payload['links']

    @property
    def status(self):
        return self._payload['status']

    @property
    def last_read_date(self):
        return valid_data_types.datetime_from_iso(self._payload['last_read_date'])

    @property
    def vertices(self):
        return self._payload['vertices']

    @property
    def edges(self):
        return self._payload['edges']

    def update(self,payload):
        if self._payload and self.uri != payload['uri']:
            msg = "Invalid payload, graph URI mismatch %s when expecting %s" \
                % (payload['uri'], self.uri)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload=payload
