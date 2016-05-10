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
HTTP methods
"""
import json
import requests
import logging
logger = logging.getLogger(__name__)

import ssl
import trustedanalytics.rest.config as config
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager

requests.packages.urllib3.disable_warnings()

class Tlsv1HttpAdapter(HTTPAdapter):
    """"Transport adapter" that allows us to use TLSv1."""
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=connections,
                                       maxsize=maxsize,
                                       block=block,
                                       ssl_version=ssl.PROTOCOL_TLSv1)

# A simple http session wrapper over requests given a scheme (http or https)
class httpSession(object):
    def __init__(self, scheme):
        self.scheme = scheme
        self.session = requests.Session()
        self.session.mount('%s://' % self.scheme, Tlsv1HttpAdapter(max_retries=config.requests_defaults.max_retries))
    def __enter__(self):
        return self.session
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

# Helper methods

def _get_arg(key, from_kwargs, default):
    """Remove value from kwargs and return it, else return default value"""
    if key in from_kwargs:
        arg = from_kwargs[key]
        del from_kwargs[key]
        return arg
    return default


def _get_headers(server, from_kwargs):
    """Helper function to collect headers from kwargs when the http caller wants to override them"""
    return _get_arg('headers', from_kwargs, server.headers)


# HTTP methods

def get(server, uri_path, **kwargs):
    uri = server.create_full_uri(uri_path)
    headers = _get_headers(server, kwargs)
    timeout = _get_arg('timeout', kwargs, None)
    if logger.level <= logging.INFO:
        details = uri
        if logger.level <= logging.DEBUG:
            details += "\nheaders=%s" % headers
        logger.info("[HTTP Get] %s", details)
    with httpSession(server.scheme) as session:
        response = session.get(uri, headers=headers, timeout=timeout, verify=False, **kwargs)
    if logger.level <= logging.DEBUG:
        logger.debug("[HTTP Get Response %s] %s\nheaders=%s", response.status_code, response.text, response.headers)
    return response


def delete(server, uri_path, **kwargs):
    uri = server.create_full_uri(uri_path)
    headers = _get_headers(server, kwargs)
    logger.info("[HTTP Delete] %s", uri)
    with httpSession(server.scheme) as session:
        response = session.delete(uri, headers=headers, verify=False, **kwargs)
    if logger.level <= logging.DEBUG:
        logger.debug("[HTTP Delete Response %s] %s", response.status_code, response.text)
    return response


def post(server, uri_path, data, **kwargs):
    uri = server.create_full_uri(uri_path)
    headers = _get_headers(server, kwargs)
    if logger.level <= logging.INFO:
        try:
            pretty_data = json.dumps(json.loads(data), indent=2)
        except:
            pretty_data = data
        logger.info("[HTTP Post] %s\n%s\nheaders=%s", uri, pretty_data, headers)
    with httpSession(server.scheme) as session:
            response = session.post(uri, headers=headers, data=data, verify=False, **kwargs)
    if logger.level <= logging.DEBUG:
        logger.debug("[HTTP Post Response %s] %s", response.status_code, response.text)
    return response
