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
Spark-specific implementation on the client-side
"""
# TODO - remove client knowledge of spark, delete this file

import base64
import os

spark_home = os.getenv('SPARK_HOME')
if not spark_home:
    spark_home = '~/TrustedAnalytics/spark'
    os.environ['SPARK_HOME'] = spark_home

spark_python = os.path.join(spark_home, 'python')
import sys
if spark_python not in sys.path:
    sys.path.append(spark_python)

from serializers import PickleSerializer, BatchedSerializer, write_int

from trustedanalytics.core.row import Row, MutableRow
from trustedanalytics.core.atktypes import valid_data_types, numpy_to_bson_friendly

import bson

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


def get_group_by_aggregator_function(aggregator_row_function, data_types):
    """Wraps the UDF into new function and returns it"""
    def aggregate(acc, row):
        accumulator_wrapper = acc
        aggregator_row_function(accumulator_wrapper, row)
        acc_data = accumulator_wrapper._get_data()
        data = []
        for i, data_type in enumerate(data_types):
            try:
                value = acc_data[i]
            except TypeError as e:
                raise RuntimeError("UDF returned non-indexable value. Provided schema indicated an Indexable return type")
            except IndexError as e:
                raise RuntimeError("UDF return value did not match the number of items in the provided schema")
            cast_value = valid_data_types.cast(value, data_type)
            data.append(cast_value)
        return data
    return aggregate


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
        self._set_data(data)

def _wrap_aggregator_rows_function(frame, aggregator_function, key_indices, aggregator_schema, init_acc_values, optional_schema=None):
    """
    Wraps a python row function, like one used for a filter predicate, such
    that it will be evaluated with using the expected 'row' object rather than
    whatever raw form the engine is using.  Ideally, this belong in the engine
    """
    row_schema = optional_schema if optional_schema is not None else frame.schema
    acc_schema = valid_data_types.standardize_schema(aggregator_schema)

    if init_acc_values is None:
        init_acc_values = valid_data_types.get_default_data_for_schema(acc_schema)
    else:
        init_acc_values = valid_data_types.validate_data(acc_schema, init_acc_values)

    acc_wrapper = MutableRow(aggregator_schema)
    row_wrapper = RowWrapper(row_schema)
    key_indices_wrapper = key_indices
    def rows_func(rows):
        try:
            bson_data = bson.decode_all(rows)[0]
            rows_data = bson_data['array']
            #key_indices = bson_data['keyindices']
            acc_wrapper._set_data(list(init_acc_values))
            for row in rows_data:
                row_wrapper.load_row(row)
                aggregator_function(acc_wrapper, row_wrapper)
            result = []
            for key_index in key_indices_wrapper:
                answer = rows_data[0][key_index]
                result.append(answer)
            result.extend(acc_wrapper._get_data())
            return numpy_to_bson_friendly(result)
        except Exception as e:
            try:
                e_msg = unicode(e)
            except:
                e_msg = u'<unable to get exception message>'
            try:
                e_row = unicode(bson.decode_all(rows)[0]['array'])
            except:
                e_row = u'<unable to get row data>'
            try:
                msg = base64.urlsafe_b64encode((u'Exception: %s running UDF on row: %s' % (e_msg, e_row)).encode('utf-8'))
            except:
                msg = base64.urlsafe_b64encode(u'Exception running UDF, unable to provide details.'.encode('utf-8'))
            raise IaPyWorkerError(msg)
    return rows_func


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
            xdata = bson.decode_all(row)[0]['array']
            row_wrapper.load_row(xdata)
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
