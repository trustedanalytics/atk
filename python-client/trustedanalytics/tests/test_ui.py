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


import iatest
iatest.init()

import unittest
import trustedanalytics as ta
import trustedanalytics.core.ui as ui




abc_schema = [('a', int), ('b', unicode), ('c', unicode)]
two_abc_rows = [[1, "sixteen_16_abced", "long"],
                [2, "tiny", "really really really really long"]]

schema1 = [('i32', ta.int32),
            ('floaties', ta.float64),
            ('long_column_name_ugh_and_ugh', str),
            ('long_value', str),
            ('s', str)]

rows1 = [
    [1,
     3.14159265358,
     'a',
     '''The sun was shining on the sea,
Shining with all his might:
He did his very best to make
The billows smooth and bright--
And this was odd, because it was
The middle of the night.

The moon was shining sulkily,
Because she thought the sun
Had got no business to be there
After the day was done--
"It's very rude of him," she said,
"To come and spoil the fun!"''',
    'one'],
    [2,
     8.014512183,
     'b',
     '''I'm going down.  Down, down, down, down, down.  I'm going down.  Down, down, down, down, down.  I'm going down.  Down, down, down, down, down.  I'm going down.  Down, down, down, down, down.''',
    'two'],
    [32,
     1.0,
     'c',
     'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
     'thirty-two']]


class TestUi(unittest.TestCase):

    def test_round(self):
        self.assertEqual("3.14", ui.round_float(3.1415, ta.float32, 2))
        self.assertEqual("6.28", ui.round_float(6.2830, ta.float64, 2))
        self.assertEqual("867.5309000000", ui.round_float(867.5309, ta.float64, 10))
        self.assertEqual("867.5310000000", ui.round_float(867.5309, ta.float32, 10))  # notice quirk with float32
        self.assertEqual("[1.23, 5.68]", ui.round_vector([1.234, 5.6789], 2))
        self.assertEqual("[1.0, 2.0, 3.0]", ui.round_vector([1.234, 2.36789, 3], 0))
        self.assertEqual("[1.2, 2.4, 3.0]", ui.round_vector([1.234, 2.36789, 3], 1))

    def test_truncate(self):
        self.assertEqual("encyclopedia", ui.truncate("encyclopedia", 13))
        self.assertEqual("encyclopedia", ui.truncate("encyclopedia", 12))
        self.assertEqual("encyclop...", ui.truncate("encyclopedia", 11))
        self.assertEqual("en...", ui.truncate("encyclopedia", 5))
        self.assertEqual("e...", ui.truncate("encyclopedia", 4))
        self.assertEqual("...", ui.truncate("encyclopedia", 3))
        try:
            ui.truncate("encyclopedia", 2)
        except ValueError as e:
            pass
        else:
            self.fail("Expected value error for target_len too small")

    def test_get_col_sizes1(self):
        result = ui._get_col_sizes(two_abc_rows, 0, row_count=2, header_sizes=ui._get_header_entry_sizes(abc_schema, False), formatters=[ui.identity for i in xrange(len(abc_schema))])
        expected = [1, 16,  32]
        self.assertEquals(expected, result)

    def go_get_num_cols(self, width, expected):
        result = ui._get_col_sizes(two_abc_rows, 0, row_count=2, header_sizes=ui._get_header_entry_sizes(abc_schema, False), formatters=[ui.identity for i in xrange(len(abc_schema))])

        def get_splits(width):
            num_cols_0 = ui._get_num_cols(abc_schema, width, 0, result, 0)
            num_cols_1 = ui._get_num_cols(abc_schema, width, num_cols_0, result, 0)
            num_cols_2 = ui._get_num_cols(abc_schema, width, num_cols_0 + num_cols_1, result, 0)
            return num_cols_0, num_cols_1, num_cols_2

        self.assertEquals(expected, get_splits(width))

    def test_get_num_cols_12(self):
        self.go_get_num_cols(12, (1, 1, 1))

    def test_get_num_cols_24(self):
        self.go_get_num_cols(24, (2, 1, 0))

    def test_get_num_cols_80(self):
        self.go_get_num_cols(80, (3, 0, 0))

    def test_get_row_clump_count(self):
        row_count = 12
        wraps = [(12, 1), (11, 2), (10, 2), (6, 2), (5, 3), (4, 3), (3, 4), (2, 6), (1, 12), (13, 1), (100, 1)]
        for w in wraps:
            wrap = w[0]
            expected = w[1]
            result = ui._get_row_clump_count(row_count, wrap)
            self.assertEqual(expected, result, "%s != %s for wrap %s" % (result, expected, wrap))

    def test_simple_stripes(self):
        result = repr(ui.RowsInspection(two_abc_rows, abc_schema, offset=0, format_settings=ui.InspectSettings(wrap='stripes', margin=10)))
        expected = '''[0]
a=1
b=sixteen_16_abced
c=long
[1]
a=2
b=tiny
c=really really really really long'''
        self.assertEqual(expected, result)

    def test_wrap_long_str_1(self):
        r = [['12345678901234567890123456789012345678901234567890123456789012345678901234567890']]
        s = [('s', str)]
        settings = ui.InspectSettings(wrap=5)
        result = repr(ui.RowsInspection(r, s, offset=0, format_settings=settings))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        expected = '''[#]  s
================================================================================
[0]  12345678901234567890123456789012345678901234567890123456789012345678901234567890'''
        self.assertEqual(expected, result)

    def test_empty(self):
        r = []
        s = abc_schema
        result = repr(ui.RowsInspection(r, s, offset=0, format_settings=ui.InspectSettings(wrap=5, width=80)))
        expected = '''[##]  a  b  c
============='''
        self.assertEqual(expected, result)
        result = repr(ui.RowsInspection(r, s, offset=0, format_settings=ui.InspectSettings(wrap=5, width=80, with_types=True)))
        expected = '''[##]  a:int32  b:unicode  c:unicode
==================================='''
        self.assertEqual(expected, result)

    def test_empty_stripes(self):
        r = []
        s = schema1
        result = repr(ui.RowsInspection(r, s, offset=0, format_settings=ui.InspectSettings(wrap='stripes', width=80)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        expected = '''[0]--------------------------
i32                         =
floaties                    =
long_column_name_ugh_and_ugh=
long_value                  =
s                           ='''
        self.assertEqual(expected, result)
        result = repr(ui.RowsInspection(r, s, offset=0, format_settings=ui.InspectSettings(wrap='stripes', width=80, with_types=True)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        expected = '''[0]----------------------------------
i32:int32                           =
floaties:float64                    =
long_column_name_ugh_and_ugh:unicode=
long_value:unicode                  =
s:unicode                           ='''
        self.assertEqual(expected, result)

    def test_line_numbers(self):
        r = [[x, 'b%s' % x, None] for x in xrange(10)]
        s = abc_schema
        result = repr(ui.RowsInspection(r, s, offset=92, format_settings=ui.InspectSettings(wrap=5, width=80)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        expected = '''[##]  a  b   c
=================
[92]  0  b0  None
[93]  1  b1  None
[94]  2  b2  None
[95]  3  b3  None
[96]  4  b4  None


[###]  a  b   c
==================
[97]   5  b5  None
[98]   6  b6  None
[99]   7  b7  None
[100]  8  b8  None
[101]  9  b9  None'''
        self.assertEqual(expected, result)

    def test_inspection(self):
        result = repr(ui.RowsInspection(rows1, schema1, offset=0, format_settings=ui.InspectSettings(wrap=2, truncate=40, width=80)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        expected = '''[#]  i32  floaties       long_column_name_ugh_and_ugh
=====================================================
[0]    1  3.14159265358  a
[1]    2    8.014512183  b

[#]  long_value                                s
==================================================
[0]  The sun was shining on the sea,           one
     Shini...
[1]  I'm going down.  Down, down, down, do...  two


[#]  i32  floaties  long_column_name_ugh_and_ugh
================================================
[2]   32       1.0  c

[#]  long_value                                s
=========================================================
[2]  AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  thirty-two'''
        self.assertEqual(expected, result)

    def test_inspect_round(self):
        schema = [('f32', ta.float32), ('f64', ta.float64), ('v', ta.vector(2))]
        rows = [[0.1234, 9.87654321, [1.0095, 2.034]],
                [1234.5, 9876.54321, [99.999, 33.33]]]
        result = repr(ui.RowsInspection(rows, schema, offset=0, format_settings=ui.InspectSettings(wrap=2, round=2)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        expected = '''[#]  f32      f64      v
======================================
[0]     0.12     9.88  [1.01, 2.03]
[1]  1234.50  9876.54  [100.00, 33.33]'''
        self.assertEqual(expected, result)

        result = repr(ui.RowsInspection(rows, schema, offset=0, format_settings=ui.InspectSettings(wrap='stripes', round=3)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        expected = '''[0]-
f32=0.123
f64=9.877
v  =[1.010, 2.034]
[1]-
f32=1234.500
f64=9876.543
v  =[99.999, 33.330]'''
        self.assertEqual(expected, result)

    def test_inspect_settings_reset(self):
        settings = ui.InspectSettings()
        repr1 = repr(settings)
        settings.wrap = 10
        settings.truncate = 8
        settings.round = 2
        settings.width = 90
        settings.margin = 4
        settings.with_types = True
        settings.reset()
        repr2 = repr(settings)
        self.assertEqual(repr1, repr2)

    def test_copy(self):
        settings1 = ui.InspectSettings(truncate=8, width=40)
        settings2 = settings1.copy(round=4, width=80)
        self.assertEqual(8, settings2.truncate)
        self.assertEqual(80, settings2.width)
        self.assertEqual(4, settings2.round)

    def test_inspect_nones(self):
        schema = [('s', str), ('v', ta.vector(2))]
        rows = [['super', [1.0095, 2.034]],
                [None, None]]
        result = repr(ui.RowsInspection(rows, schema, offset=0, format_settings=ui.InspectSettings(wrap=2, round=2, truncate=4)))
        result = '\n'.join([line.rstrip() for line in result.splitlines()])
        print result

    def test_neg_inspect_settings(self):
        try:
            ui.RowsInspection(1, [], 0, format_settings='jump')
        except TypeError:
            pass
        else:
            self.fail("Expected TypeError")

    def test_settings_wrap(self):
        settings = ui.InspectSettings()
        settings.wrap = 10
        try:
            settings.wrap = 0
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.wrap = 'stripe'
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.wrap = 3.5
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")

    def test_settings_truncate(self):
        settings = ui.InspectSettings()
        settings.truncate = 4
        try:
            settings.truncate = 0
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.truncate = '12'  # no strings
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.truncate = 3.5  # no floats
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")

    def test_settings_round(self):
        settings = ui.InspectSettings()
        settings.round = 4
        settings.round = 0
        try:
            settings.round = -1
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.round = '12'  # no strings
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.round = 3.5  # no floats
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")

    def test_settings_width(self):
        settings = ui.InspectSettings()
        settings.width = 4
        try:
            settings.width = 0
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.width = '12'  # no strings
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.width = 3.5  # no floats
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")

    def test_settings_margin(self):
        settings = ui.InspectSettings()
        settings.margin = 4
        try:
            settings.margin = 0
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.margin = -1
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.margin = '12'  # no strings
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.margin = 3.5  # no floats
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")

    def test_settings_with_types(self):
        settings = ui.InspectSettings()
        settings.with_types = True
        settings.with_types = False
        settings.with_types = None
        try:
            settings.with_types = 0
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.with_types = '12'  # no strings
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")
        try:
            settings.with_types = 3.5  # no floats
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError")


if __name__ == '__main__':
    unittest.main()
