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

from trustedanalytics.core.atktypes import *
from trustedanalytics.rest.frame import FrameSchema, FrameData

class TestConnect(unittest.TestCase):

    def test_meta(self):
        import trustedanalytics as ta
        ta.connect()

class TestInspectionTable(unittest.TestCase):

    def test_get_schema_for_selected_columns(self):
        schema = [('user_id', int32), ('vertex_type', str), ('movie_id', int32), ('rating', int32), ('splits', str)]
        selected_schema = FrameSchema.get_schema_for_columns(schema, ['user_id', 'splits'])
        self.assertEqual(selected_schema, [('user_id', int32), ('splits', str)])

    def test_get_schema_for_selected_columns_change_order(self):
        schema = [('user_id', int32), ('vertex_type', str), ('movie_id', int32), ('rating', int32), ('splits', str)]
        selected_schema = FrameSchema.get_schema_for_columns(schema, ['splits', 'user_id', 'rating'])
        self.assertEqual(selected_schema, [('splits', str), ('user_id', int32), ('rating', int32)])

    def test_get_indices_for_selected_columns(self):
        schema = [('user_id', int32), ('vertex_type', str), ('movie_id', int32), ('rating', int32), ('splits', str)]
        indices = FrameSchema.get_indices_for_selected_columns(schema, ['user_id', 'splits'])
        self.assertEqual(indices, [0, 4])

    def test_extract_columns_from_data(self):
        indices = [0, 2]
        data = [[1, 'a', '3'], [2, 'b', '2'], [3, 'c', '5'], [4, 'd', '-10']]
        result = FrameData.extract_data_from_selected_columns(data, indices)
        self.assertEqual(result, [[1, '3'], [2, '2'], [3, '5'], [4, '-10']])


if __name__ == '__main__':
    unittest.main()
