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

import iatest
iatest.init()

import unittest

from trustedanalytics.tests.sources import SimpleDataSource
from trustedanalytics.core.atktypes import *

expected_repr_123 = """   a      b
0  1    one
1  2    two
2  3  three"""


class TestSimpleDataSource(unittest.TestCase):

    def test_to_pandas_dataframe_using_rows(self):
        sds = SimpleDataSource(schema=[('a', int32), ('b', str)], rows=[(1, 'one'), (2, 'two'), (3, 'three')])
        df = sds.to_pandas_dataframe()
        result = repr(df)
        self.assertEquals(expected_repr_123, result)

    def test_to_pandas_dataframe_using_columns(self):
        sds = SimpleDataSource(schema=[('a', int32), ('b', str)],
                               columns={'a': [1, 2, 3],
                                        'b': ['one', 'two', 'three']})
        df = sds.to_pandas_dataframe()
        result = repr(df)
        self.assertEquals(expected_repr_123, result)


if __name__ == '__main__':
    unittest.main()
