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
from pytz import utc


class ValidDataTypes(unittest.TestCase):

    def test_contains(self):
        self.assertTrue(int32 in valid_data_types)
        self.assertTrue(float64 in valid_data_types)
        self.assertFalse(dict in valid_data_types)  # not supported yet!
        self.assertFalse(list in valid_data_types)  # not supported yet!
        self.assertTrue(int in valid_data_types)
        self.assertTrue(float in valid_data_types)
        self.assertTrue(ignore in valid_data_types)
        self.assertFalse(unknown in valid_data_types)
        self.assertTrue(datetime in valid_data_types)

    def test_repr(self):
        r = valid_data_types.__repr__()
        self.assertEqual("""datetime, float32, float64, ignore, int32, int64, unicode, vector(n)
(and aliases: float->float64, int->int32, long->int64, str->unicode)""", r)

    def test_get_from_string(self):
        self.assertEqual(int64, valid_data_types.get_from_string("int64"))
        self.assertEqual(int32, valid_data_types.get_from_string("int32"))
        self.assertEqual(unicode, valid_data_types.get_from_string("str"))
        self.assertEqual(datetime, valid_data_types.get_from_string("datetime"))
        for bad_str in ["string"]:
            try:
                valid_data_types.get_from_string(bad_str)
            except ValueError:
                pass
            else:
                self.fail("Expected exception!")

    def test_get_from_type(self):
        self.assertEqual(int64, valid_data_types.get_from_type(int64))
        self.assertEqual(float64, valid_data_types.get_from_type(float))
        self.assertEqual(ignore, valid_data_types.get_from_type(ignore))
        self.assertEqual(datetime, valid_data_types.get_from_type(datetime))

    def test_validate(self):
        valid_data_types.validate(float64)
        valid_data_types.validate(int)
        valid_data_types.validate(ignore)
        valid_data_types.validate(datetime)

    def test_to_string(self):
        self.assertEqual('int32', valid_data_types.to_string(int32))
        self.assertEqual('float64', valid_data_types.to_string(float64))
        self.assertEqual('unicode', valid_data_types.to_string(str))
        self.assertEqual('ignore', valid_data_types.to_string(ignore))
        self.assertEqual('datetime', valid_data_types.to_string(datetime))

    def test_cast(self):
        self.assertEqual(float32(1.0), valid_data_types.cast(1.0, float32))
        self.assertEqual('jim', valid_data_types.cast('jim', str))
        self.assertTrue(valid_data_types.cast(None, unicode) is None)
        self.assertEqual(datetime(2010, 5, 8, 23, 41, 54, tzinfo=utc), valid_data_types.cast("2010-05-08T23:41:54.000Z", datetime))
        self.assertEqual(valid_data_types.datetime_from_iso("2015-09-08T15:51:22"), valid_data_types.cast("2015-09-08T15:51:22", datetime))
        try:
            valid_data_types.cast(3, set)
        except ValueError:
            pass
        else:
            self.fail("Expected exception!")

    def test_cast_vector(self):
        v = valid_data_types.cast([2.0, 5.0], vector(2))
        self.assertTrue(v[0] == 2.0)
        self.assertTrue(v[1] == 5.0)
        v = valid_data_types.cast(2.0, vector(1))
        self.assertTrue(len(v) == 1)
        self.assertEqual(2.0, v[0])
        v = valid_data_types.cast("3.14, 6.28, 9.42", vector(3))
        self.assertTrue(len(v) == 3)
        self.assertEqual(3.14, v[0])
        self.assertEqual(9.42, v[2])
        v = valid_data_types.cast("[3.14, 6.28, 9.42]", vector(3))
        self.assertTrue(len(v) == 3)
        self.assertEqual(3.14, v[0])
        self.assertEqual(9.42, v[2])

    def test_datetime(self):
        from dateutil.tz import tzoffset
        self.assertEqual(datetime(2015, 9, 8).isoformat(), "2015-09-08T00:00:00")
        self.assertEqual(datetime(2015, 9, 8, tzinfo=tzoffset(None, 6*60*60)).isoformat(), "2015-09-08T00:00:00+06:00")
        self.assertEqual(datetime(2015, 9, 8, tzinfo=tzoffset(None, -7*60*60-30*60)).isoformat(), "2015-09-08T00:00:00-07:30")

    def test_nan(self):
        import numpy as np
        self.assertTrue(valid_data_types.cast(np.nan, float32) is None)

    def test_positive_inf(self):
        import numpy as np
        self.assertTrue(valid_data_types.cast(np.inf, float64) is None)

    def test_negative_inf(self):
        import numpy as np
        self.assertTrue(valid_data_types.cast(-np.inf, float32) is None)

    def test_native_float_values(self):
        self.assertTrue(valid_data_types.cast(float('nan'), float32) is None)
        self.assertTrue(valid_data_types.cast(float('NaN'), float32) is None)
        self.assertTrue(valid_data_types.cast(float('inf'), float32) is None)
        self.assertTrue(valid_data_types.cast(float('Infinity'), float64) is None)
        self.assertTrue(valid_data_types.cast(float('-inf'), float64) is None)
        self.assertTrue(valid_data_types.cast(float('-Infinity'), float64) is None)

    def test_overflow(self):
        import numpy as np
        self.assertTrue(valid_data_types.cast(np.float64(2 ** 1000), float32) is None)
        self.assertTrue(valid_data_types.cast(-np.float64(2 ** 1000), float32) is None)

    def test_standardize_schema(self):
        s1 = [('a', str), ('b', int)]
        expected1 = [('a', unicode), ('b', int32)]
        results1 = valid_data_types.standardize_schema(s1)
        self.assertEqual(expected1, results1)

    def test_get_default_type_value(self):
        self.assertEqual(0, valid_data_types.get_default_type_value(int32))
        self.assertEqual(0, valid_data_types.get_default_type_value(int64))
        self.assertEqual("", valid_data_types.get_default_type_value(unicode))
        self.assertEqual(0.0, valid_data_types.get_default_type_value(float32))
        self.assertEqual(0.0, valid_data_types.get_default_type_value(float64))
        try:
            valid_data_types.get_default_type_value(str)
        except ValueError as e:
            self.assertTrue("Unable to find default value" in str(e))
        else:
            self.fail("Exception expected")

    def test_get_default_data_for_schema(self):
        s = [('a', unicode), ('b', int32)]
        expected = ["", 0]
        self.assertEqual(expected, valid_data_types.get_default_data_for_schema(s))

    def test_validate_data(self):
        s = [('a', str), ('f', float)]
        d = ["string", 3.14]
        results = valid_data_types.validate_data(s, d)
        self.assertEqual(d, results)

if __name__ == '__main__':
    unittest.main()
