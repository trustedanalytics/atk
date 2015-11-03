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

import trustedanalytics.rest.http as http
import trustedanalytics.rest.config as config
from trustedanalytics.core.api import api_status
import json


class Server(object):
    """
    Base class for server objects
    """
    _unspecified = object()  # sentinel

    @staticmethod
    def _get_value_from_config(value_name, default=_unspecified):
        try:
            value = getattr(config.server_defaults, value_name)
        except AttributeError:
            if default is Server._unspecified:
                raise RuntimeError("Unable to find value '%s' for server in configuration" % value_name)
            value = default
        return value

    def __init__(self, uri, scheme, headers, user=None):
        self._uri = uri
        self._scheme = scheme
        self._headers = headers
        self._user = user
        self._conf_frozen = None

    def _repr_attrs(self):
        return ["uri", "scheme", "headers", "user"]

    def __repr__(self):
        d = dict([(a, getattr(self, a)) for a in self._repr_attrs()])
        return json.dumps(d, cls=ServerJsonEncoder, sort_keys=True, indent=2)

    @staticmethod
    def _str_truncate_value(v):
        """helper method for __str__"""
        s = str(v)
        if len(s) > 58:
            return s[:58] + "..."
        return s

    def __str__(self):
        """This is a friendlier str representation, which truncates long lines"""
        import json
        d = json.loads(repr(self))
        line = "\n------------------------------------------------------------------------------\n"
        return line + "\n".join(["%-15s: %s" % (k, self._str_truncate_value(v)) for k, v in sorted(d.items())]) + line

    def _error_if_conf_frozen_and_not_eq(self, a, b):
        if a != b:
            self._error_if_conf_frozen()

    def _error_if_conf_frozen(self):
        if api_status.is_installed:
            raise RuntimeError("This server's connection settings can no longer be modified.")

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        #with api_status._api_lock:
        self._error_if_conf_frozen_and_not_eq(self._uri, value)
        self._uri = value

    @property
    def host(self):
        """server host name"""
        return HostPortHelper.get_host_port(self._uri)[0]

    @host.setter
    def host(self, value):
        #with api_status._api_lock:
        uri = self._uri
        new_uri = HostPortHelper.set_uri_host(uri, value)
        self._error_if_conf_frozen_and_not_eq(uri, new_uri)
        self._uri = new_uri

    @property
    def port(self):
        """server port number - can be None for no specification"""
        return HostPortHelper.get_host_port(self._uri)[1]

    @port.setter
    def port(self, value):
        #with api_status._api_lock:
        uri = self._uri
        new_uri = HostPortHelper.set_uri_port(uri, value)
        #self._error_if_conf_frozen_and_not_eq(uri, new_uri)  # todo: enable when api_status lock figured out
        self._uri = new_uri

    @property
    def scheme(self):
        """connection scheme, like http or https"""
        return self._scheme

    @scheme.setter
    def scheme(self, value):
        #with api_status._api_lock:
        self._error_if_conf_frozen()
        self._scheme = value

    @property
    def headers(self):
        """scheme headers"""
        return self._headers

    @headers.setter
    def headers(self, value):
        #with api_status._api_lock:
        self._error_if_conf_frozen()
        self._headers = value

    def _get_scheme_and_authority(self):
        return "%s://%s" % (self.scheme, self.uri)

    def _get_base_uri(self):
        """Returns the base uri used by client as currently configured to talk to the server"""
        return self._get_scheme_and_authority()

    def create_full_uri(self, uri_path=''):
        base = self._get_base_uri()
        if uri_path.startswith('http'):
            return uri_path
        if len(uri_path) > 0 and uri_path[0] != '/':
            uri_path = '/' + uri_path
        return base + uri_path

    def _check_response(self, response):
        if 400 <= response.status_code < 600 or response.status_code == 202:
                raise RuntimeError(response.text)

    def get(self, url):
        response = http.get(self, url)
        self._check_response(response)
        return response

    def post(self, url, data):
        if not isinstance(data, basestring):
            data = json.dumps(data)
        response = http.post(self, url, data)
        self._check_response(response)
        return response

    def delete(self, url):
        response = http.delete(self, url)
        self._check_response(response)
        return response


class ServerJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Server):
            return json.loads(repr(obj))
        return json.JSONEncoder.default(self, obj)


class HostPortHelper(object):
    """
    Converts Host/Port strings to URI strings
    """
    import re
    pattern = re.compile(r'(.*?)(:(\d+))?$')

    @staticmethod
    def get_host_port(uri):
        """gets host and port from a uri"""
        match = HostPortHelper.pattern.search(uri)
        if not match:
            raise ValueError("Bad uri string %s" % uri)
        host, option, port = match.groups()
        return host, port

    @staticmethod
    def get_uri(host, port):
        """gets uri string from host and port"""
        if port:
            return '%s:%s' % (host, port)
        return host

    @staticmethod
    def set_uri_host(uri, new_host):
        """returns the uri with the host replaced.  Returns None if new_host is None"""
        if not new_host:
            return None
        host, port = HostPortHelper.get_host_port(uri)
        return HostPortHelper.get_uri(new_host, port)

    @staticmethod
    def set_uri_port(uri, new_port):
        """returns the uri with the port added or replaced, or removed if new_port is None"""
        host, port = HostPortHelper.get_host_port(uri)
        return HostPortHelper.get_uri(host, new_port)



