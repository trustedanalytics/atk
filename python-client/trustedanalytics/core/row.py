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

from collections import OrderedDict
from trustedanalytics.core.atktypes import valid_data_types

class Row(object):

    def __init__(self, schema, data=None):
        """
        Expects schema to as list of tuples
        """

        # Can afford a richer object since it will be reused per row, with more init up front to save calculation
        standardized_schema = [(name, valid_data_types.get_from_type(t)) for name, t in schema]
        self.__schema_dict = OrderedDict(standardized_schema)
        self.__data = [] if data is None else data  # data is an array of strings right now
        self.__dtypes = self.__schema_dict.values()
        self.__indices_dict = dict([(k, i) for i, k, in enumerate(self.__schema_dict.keys())])
        self.__dtype_constructors = [valid_data_types.get_constructor(t) for t in self.__dtypes]

    def __getattr__(self, name):
        if name != "_Row__schema_dict" and name in self.__schema_dict.keys():
            return self._get_cell_value(name)
        return super(Row, self).__getattribute__(name)

    def __getitem__(self, key):
        try:
            if isinstance(key, int):
                return self._get_cell_value_by_index(key)
            if isinstance(key, slice):
                raise TypeError("Index slicing a row is not supported")
            if isinstance(key, list):
                return [self._get_cell_value(k) for k in key]
            return self._get_cell_value(key)
        except KeyError:
            raise KeyError("Column name " + str(key) + " not present.")

    def __len__(self):
        return len(self.__schema_dict)

    def __iter__(self):
        return self.items().__iter__()

    def _get_data(self):
        return self.__data

    def _set_data(self, value):
        self.__data = value

    def keys(self):
        return self.__schema_dict.keys()

    def values(self):
        return [self._get_cell_value(k) for k in self.keys()]

    def types(self):
        return self.__schema_dict.values()

    def items(self):
        return zip(self.keys(), self.values())

    def get_cell_type(self, key):
        try:
            return self.__schema_dict[key]
        except KeyError:
            raise ValueError("'%s' is not in the schema" % key)

    def _get_cell_value(self, key):
        try:
            index = self.__indices_dict[key]
        except ValueError:
            raise KeyError(key)
        return self._get_cell_value_by_index(index)

    def _get_cell_value_by_index(self, index):
        try:
            return self.__dtype_constructors[index](self.__data[index])
        except IndexError:
            raise IndexError("Internal Error: improper index %d used in schema with %d columns" % (index, len(self.__schema_dict)))
