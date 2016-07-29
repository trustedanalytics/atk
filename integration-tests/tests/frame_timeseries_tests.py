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

import unittest
import trustedanalytics as ta
import uuid         # for generating unique frame names

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class FrameTimeSeriesTest(unittest.TestCase):
    """
    Test frame time series statistical tests
    """
    _multiprocess_can_split_ = True


    def test_adf_params(self):
        """
        Test the Augmented Dickey-Fuller test with invalid parameters
        """
        data = [[12.88969427], [13.54964408], [13.8432745], [12.13843611], [12.81156092], [14.2499628], [15.12102595]]
        frame = ta.Frame(ta.UploadRows(data, [("data", ta.float32)]))

        # Test calling ADF test with and without regression parameter
        self.assertNotEqual(frame.timeseries_augmented_dickey_fuller_test("data", 0), None)
        self.assertNotEqual(frame.timeseries_augmented_dickey_fuller_test("data", 0, "c"), None)

        try:
            frame.timeseries_augmented_dickey_fuller_test("data", 0, "bogus")
        except Exception as e:
            assert("bogus is not c, ct, or ctt" in e.message)

    def test_adf_column_types(self):
        """
        Tests the Augmented Dickey-Fuller test with different column types
        """
        data = [[1, "a", 1.5], [2, "b", 18.5], [4, "c", 22.1], [5, "d", 19.0], [7, "e", 25.6], [8, "f", 36.75]]
        schema = [("int_column", ta.int32), ("str_column", str), ("float_column", ta.float32)]
        frame = ta.Frame(ta.UploadRows(data, schema))

        try:
            # string column should have an error
            frame.timeseries_augmented_dickey_fuller_test("str_column", 0)
            raise RuntimeError("Expected error since the str_column is not numerical.")
        except Exception as e:
            assert("Column str_column was not numerical" in e.message)

        # Numerical columns should not have an error
        self.assertNotEqual(frame.timeseries_augmented_dickey_fuller_test("int_column", 0), None)
        self.assertNotEqual(frame.timeseries_augmented_dickey_fuller_test("float_column", 0), None)

    def test_dwtest_column_types(self):
        """
        Tests that the Durbin-Watson test only works with numerical columns
        """
        data = [[1, "a", 1.5], [2, "b", 18.5], [4, "c", 22.1], [5, "d", 19.0], [7, "e", 25.6], [8, "f", 36.75]]
        schema = [("int_column", ta.int32), ("str_column", str), ("float_column", ta.float32)]
        frame = ta.Frame(ta.UploadRows(data, schema))

        try:
            # calling durbin-watson with a string column should fail
            frame.timeseries_durbin_watson_test("str_column")
            raise RuntimeError("Expected error since the column must be numerical")
        except Exception as e:
            assert("Column str_column was not numerical" in e.message)

        # int and float columns should not give any error
        self.assertNotEqual(frame.timeseries_durbin_watson_test("int_column"), None)
        self.assertNotEqual(frame.timeseries_durbin_watson_test("float_column"), None)


    def test_bgt_invalid_column(self):
        """
        Tests the Breusch-Godfrey test with non-numerical data, and expects an error
        """
        data = [[1, "a", 1.5], [2, "b", 18.5], [4, "c", 22.1], [5, "d", 19.0], [7, "e", 25.6], [8, "f", 36.75]]
        schema = [("int_column", ta.int32), ("str_column", str), ("float_column", ta.float32)]
        frame = ta.Frame(ta.UploadRows(data, schema))

        try:
            frame.timeseries_breusch_godfrey_test("str_column", ["int_column", "float_column"], max_lag=1)
            raise RuntimeError("Expected error since the y column specified has strings")
        except Exception as e:
            assert("Column str_column was not numerical" in e.message)

        try:
            frame.timeseries_breusch_godfrey_test("float_column", ["int_column", "str_column"], 1)
            raise RuntimeError("Expected error since one of the x columns specified has strings.", max_lag=1)
        except Exception as e:
            assert("Column str_column was not numerical" in e.message)

        # numerical data should not have an error
        self.assertNotEqual(frame.timeseries_breusch_godfrey_test("float_column", ["int_column"], max_lag=1), None)

    def test_bpt_invalid_column(self):
        """
        Tests the Breusch-Pagan test with non-numerical data, and expects an error
        """
        data = [[1, "a", 1.5], [2, "b", 18.5], [4, "c", 22.1], [5, "d", 19.0], [7, "e", 25.6], [8, "f", 36.75]]
        schema = [("int_column", ta.int32), ("str_column", str), ("float_column", ta.float32)]
        frame = ta.Frame(ta.UploadRows(data, schema))

        try:
            frame.timeseries_breusch_pagan_test("str_column", ["int_column", "float_column"])
            raise RuntimeError("Expected error since the y column specified has strings")
        except Exception as e:
            assert("Column str_column was not numerical" in e.message)

        try:
            frame.timeseries_breusch_pagan_test("float_column", ["int_column", "str_column"])
            raise RuntimeError("Expected error since one of the x columns specified has strings.")
        except Exception as e:
            assert("Column str_column was not numerical" in e.message)

        # numerical data should not have an error
        self.assertNotEqual(frame.timeseries_breusch_pagan_test("float_column", ["int_column"]), None)



