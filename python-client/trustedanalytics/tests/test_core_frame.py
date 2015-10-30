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
from mock import patch

from trustedanalytics import connect
from trustedanalytics.core.frame import Frame, _BaseFrame
from trustedanalytics.core.column import Column
from trustedanalytics.core.files import CsvFile
from trustedanalytics.core.atktypes import *


def get_simple_frame_abcde():
    schema = [('A', int32), ('B', int64), ('C', float32), ('D', float64), ('E', str)]
    f = Frame(CsvFile("dummy.csv", schema))
    connect()
    try:
        del _BaseFrame.schema
    except:
        pass
    setattr(f, "schema", schema)
    return f


def get_simple_frame_abfgh():
    schema = [('A', int32),  ('B', int64), ('F', float32), ('G', float64), ('H', str)]
    f = Frame(CsvFile("dummy.csv", schema))
    connect()
    try:
        del _BaseFrame.schema
    except:
        pass
    setattr(f, "schema", schema)
    return f


def fake_download(server):
    """Mock out download from server, such that connect works offline"""
    return "Test Stub", []


@patch('trustedanalytics.meta.config.get_frame_backend')
class FrameConstruction(unittest.TestCase):

    def validate_column_names(self, frame, column_names):
        self.assertEqual(len(column_names), len(frame))
        for i in column_names:
            self.assertNotEqual(None,frame[i])

    @patch("trustedanalytics.meta.installapi.download_server_details", fake_download)
    def test_create(self, patched_be):
        connect()
        f = Frame()
        self.assertEqual(None, f.uri)

    def test_create_from_csv(self, patched_be):
        connect()
        f = Frame(CsvFile("dummy.csv", [('A', int32), ('B', int64)]))
        self.assertEqual(0, len(f))
        try:
            c = f['C']
            self.fail()
        except KeyError:
            pass

    def test_slice_columns_with_list(self, patched_be):
        f = get_simple_frame_abcde()
        cols = f[['B', 'C']]
        self.assertEqual(2, len(cols))
        self.assertTrue(isinstance(cols[0], Column) and cols[0].name == 'B')
        self.assertTrue(isinstance(cols[1], Column) and cols[1].name == 'C')

    def test_iter(self, x):
        f1 = get_simple_frame_abcde()
        names = [c.name for c in f1]
        self.validate_column_names(f1, names)


if __name__ == '__main__':
    unittest.main()
