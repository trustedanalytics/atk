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
Spark-specific implementation on the client-side
"""
# TODO - remove client knowledge of spark, delete this file

import base64
import os
import itertools
from udfzip import UdfZip
from types import ModuleType

spark_home = os.getenv('SPARK_HOME')
if not spark_home:
    spark_home = '~/TrustedAnalytics/spark'
    os.environ['SPARK_HOME'] = spark_home

spark_python = os.path.join(spark_home, 'python')
import sys
if spark_python not in sys.path:
    sys.path.append(spark_python)

from serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer, CloudPickleSerializer, write_int

from trustedanalytics.core.row import Row
from trustedanalytics.core.atktypes import valid_data_types, numpy_to_bson_friendly

import bson

UdfDependencies = []


def get_file_content_as_str(filename):

    if isinstance(filename, ModuleType) and hasattr(filename, '__path__'): # Serialize modules
        UdfZip.zipdir(filename.__path__)
        name, fileToSerialize = ('%s.zip' % os.path.basename(filename), '/tmp/iapydependencies.zip')
    elif isinstance(filename, ModuleType) and hasattr(filename, '__file__'): # Serialize single file based modules
        name, fileToSerialize = (filename.__file__, filename.__file__)
    elif os.path.isdir(filename): # Serialize local directories
        UdfZip.zipdir(filename)
        name, fileToSerialize = ('%s.zip' % os.path.basename(filename), '/tmp/iapydependencies.zip')
    elif os.path.isfile(filename) and filename.endswith('.py'): # Serialize local files
        name, fileToSerialize = (filename, filename)
    else:
        raise Exception('%s should be either local python script without any packaging structure \
        or the absolute path to a valid python package/module which includes the intended python file to be included and all \
                        its dependencies.' % filename)
    # Serialize the file contents and send back along with the new serialized file names
    with open(fileToSerialize, 'rb') as f:
        return (name, base64.urlsafe_b64encode(f.read()))


def _get_dependencies(filenames):
    dependencies = []
    for filename in filenames:
        name, content = get_file_content_as_str(filename)
        dependencies.append({'file_name': name, 'file_content': content})
    return dependencies

def ifiltermap(predicate, function, iterable):
    """creates a generator than combines filter and map"""
    return (function(item) for item in iterable if predicate(item))

def ifilter(predicate, iterable):
    """Filter records and return decoded object so that batch processing can work correctly"""
    return (numpy_to_bson_friendly(bson.decode_all(item)[0]["array"]) for item in iterable if predicate(item))

def ifilterfalse(predicate, iterable):
    """Filter records that do not match predicate and return decoded object so that batch processing can encode"""
    return (numpy_to_bson_friendly(bson.decode_all(item)[0]["array"]) for item in iterable if not predicate(item))


def get_add_one_column_function(row_function, data_type):
    """Returns a function which adds a column to a row based on given row function"""
    def add_one_column(row):
        result = row_function(row)
        cast_value = valid_data_types.cast(result, data_type)
        return [numpy_to_bson_friendly(cast_value)]

    return add_one_column


def get_add_many_columns_function(row_function, data_types):
    """Returns a function which adds several columns to a row based on given row function"""
    def add_many_columns(row):
        result = row_function(row)
        data = []
        for i, data_type in enumerate(data_types):
            try:
                value = result[i]
            except TypeError as e:
                raise RuntimeError("UDF returned non-indexable value. Provided schema indicated an Indexable return type")
            except IndexError as e:
                raise RuntimeError("UDF return value did not match the number of items in the provided schema")
            cast_value = valid_data_types.cast(value, data_type)
            data.append(numpy_to_bson_friendly(cast_value))
        # return json.dumps(data, cls=NumpyJSONEncoder)
        return data
        # return bson.binary.Binary(bson.BSON.encode({"array": data}))
    return add_many_columns


def get_copy_columns_function(column_names, from_schema):
    """Returns a function which copies only certain columns for a row"""
    indices = [i for i, column in enumerate(from_schema) if column[0] in column_names]

    def project_columns(row):
        return [numpy_to_bson_friendly(row[index]) for index in indices]
    return project_columns


class IaPyWorkerError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(base64.urlsafe_b64decode(self.value))


class RowWrapper(Row):
    """
    Wraps row for specific RDD line digestion using the Row object
    """

    def load_row(self, data):
        self._set_data(bson.decode_all(data)[0]['array'])


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


def _wrap_row_function(frame, row_function, optional_schema=None):
    """
    Wraps a python row function, like one used for a filter predicate, such
    that it will be evaluated with using the expected 'row' object rather than
    whatever raw form the engine is using.  Ideally, this belong in the engine
    """
    schema = optional_schema if optional_schema is not None else frame.schema  # must grab schema now so frame is not closed over
    row_wrapper = RowWrapper(schema)
    def row_func(row):
        try:
            row_wrapper.load_row(row)
            return row_function(row_wrapper)
        except Exception as e:
            try:
                e_msg = unicode(e)
            except:
                e_msg = u'<unable to get exception message>'
            try:
                e_row = unicode(bson.decode_all(row)[0]['array'])
            except:
                e_row = u'<unable to get row data>'
            try:
                msg = base64.urlsafe_b64encode((u'Exception: %s running UDF on row: %s' % (e_msg, e_row)).encode('utf-8'))
            except:
                msg = base64.urlsafe_b64encode(u'Exception running UDF, unable to provide details.'.encode('utf-8'))
            raise IaPyWorkerError(msg)
    return row_func


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
    return make_http_ready(iteration_ready_function)


def get_udf_arg_for_copy_columns(frame, predicate_function, column_names):
    row_ready_predicate = _wrap_row_function(frame, predicate_function)
    row_ready_map = _wrap_row_function(frame, get_copy_columns_function(column_names, frame.schema))
    def iteration_ready_function(s, iterator): return ifiltermap(row_ready_predicate, row_ready_map, iterator)
    return make_http_ready(iteration_ready_function)


def make_http_ready(function):
    pickled_function = pickle_function(function)
    http_ready_function = encode_bytes_for_http(pickled_function)
    return { 'function': http_ready_function, 'dependencies':_get_dependencies(UdfDependencies)}


class IaBatchedSerializer(BatchedSerializer):
    def __init__(self):
        super(IaBatchedSerializer,self).__init__(PickleSerializer(), 1000)

    def dump_stream(self, iterator, stream):
        self.dump_stream_as_bson(self._batched(iterator), stream)

    def dump_stream_as_bson(self, iterator, stream):
        """write objects in iterator back to Scala as a byte array of bson objects"""
        for obj in iterator:
            if len(obj) > 0:
                serialized = bson.binary.Binary(bson.BSON.encode({"array": obj}))
                write_int(len(serialized), stream)
                stream.write(serialized)
