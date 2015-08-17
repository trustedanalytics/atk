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

class SimpleDataSource(object):

    annotation = "simple"

    def __init__(self, schema=None, rows=None, columns=None):
        if not ((rows is None) ^ (columns is None)):
            raise ValueError("Either rows or columns must be supplied")
        if schema and not isinstance(schema, OrderedDict):
            self.schema = OrderedDict(schema)
        else:
            self.schema = schema
        self.rows = rows
        self.columns = columns
        if columns:
            names = self.schema.keys()
            if len(names) != len(self.columns):
                raise ValueError("number of columns in schema not equals number of columns provided")
            for key in self.columns.keys():
                if key not in names:
                    raise ValueError("names in schema do not all match the names in the columns provided")

    def to_pandas_dataframe(self):
        import numpy as np
        from pandas import DataFrame
        if self.rows:
            a = np.array(self.rows, dtype=_schema_as_numpy_dtype(self.schema))
            df = DataFrame(a)
        else:  # columns
            df = DataFrame(self.columns)
        return df

def _schema_as_numpy_dtype(schema):
    return [(c, _get_numpy_dtype_from_core_type(t)) for c, t in schema.items()]

def _get_numpy_dtype_from_core_type(t):
    return object
    # if t in [str, unicode, dict, bytearray, list]:
    #     return object
    # return t
