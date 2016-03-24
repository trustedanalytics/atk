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


import base64
import itertools

from serializers import UTF8Deserializer, CloudPickleSerializer
from trustedanalytics.rest.spark import _wrap_row_function, get_copy_columns_function, IaBatchedSerializer, ifiltermap, _wrap_aggregator_rows_function
from trustedanalytics.rest.udfzip import get_dependencies_content


def pickle_function(func):
    """Pickle the function the way Pyspark does"""

    command = (func, None, UTF8Deserializer(), IaBatchedSerializer())
    pickled_function = CloudPickleSerializer().dumps(command)
    return pickled_function


def encode_bytes_for_http(b):
    """
    Encodes bytes using base64, so they can travel as a string
    """
    return base64.urlsafe_b64encode(b)

def get_udf_arg(frame, subject_function, iteration_function, optional_schema=None):
    """
    Prepares a python row function for server execution and http transmission

    Parameters
    ----------
    frame : Frame
        frame on whose rows the function will execute
    subject_function : function
        a function with a single row parameter
    iteration_function: function
        the iteration function to apply for the frame.  In general, it is
        imap.  For filter however, it is ifilter
    """
    row_ready_function = _wrap_row_function(frame, subject_function, optional_schema)
    def iterator_function(iterator): return iteration_function(row_ready_function, iterator)
    def iteration_ready_function(s, iterator): return iterator_function(iterator)
    x = make_http_ready(iteration_ready_function)
    return x

def get_udf_arg(frame, subject_function, iteration_function, optional_schema=None):
    """
    Prepares a python row function for server execution and http transmission

    Parameters
    ----------
    frame : Frame
        frame on whose rows the function will execute
    subject_function : function
        a function with a single row parameter
    iteration_function: function
        the iteration function to apply for the frame.  In general, it is
        imap.  For filter however, it is ifilter
    """
    row_ready_function = _wrap_row_function(frame, subject_function, optional_schema)
    def iterator_function(iterator): return iteration_function(row_ready_function, iterator)
    def iteration_ready_function(s, iterator): return iterator_function(iterator)
    x = make_http_ready(iteration_ready_function)
    return x



def get_aggregator_udf_arg(frame, aggregator_function, iteration_function, key_indices, aggregator_schema, init_val, optional_schema=None):
    """
    Prepares a python row function for server execution and http transmission

    Parameters
    ----------
    frame : Frame
        frame on whose rows the function will execute
    subject_function : function
        a function with a single row parameter
    iteration_function: function
        the iteration function to apply for the frame.  In general, it is
        imap.  For filter however, it is ifilter
    """
    row_ready_function = _wrap_aggregator_rows_function(frame, aggregator_function, key_indices, aggregator_schema, init_val, optional_schema)
    def iterator_function(iterator): return iteration_function(row_ready_function, iterator)
    def iteration_ready_function(s, iterator): return iterator_function(iterator)
    x = make_http_ready(iteration_ready_function)
    return x

def get_udf_arg_for_copy_columns(frame, predicate_function, column_names):
    row_ready_predicate = _wrap_row_function(frame, predicate_function)
    row_ready_map = _wrap_row_function(frame, get_copy_columns_function(column_names, frame.schema))
    def iteration_ready_function(s, iterator): return ifiltermap(row_ready_predicate, row_ready_map, iterator)
    return make_http_ready(iteration_ready_function)


def make_http_ready(function):
    pickled_function = pickle_function(function)
    http_ready_function = encode_bytes_for_http(pickled_function)
    return { 'function': http_ready_function, 'dependencies':get_dependencies_content()}



