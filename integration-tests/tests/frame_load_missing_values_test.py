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

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class FrameLoadMissingValuesTest(unittest.TestCase):
    """
    Test uses a csv file that has missing values and tests loading a frame with different datatypes.
    Ensures that the integer/float option datatype columns are able to handle missing values and
    have a proper missing value in the frame.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        self.csv_path = "/datasets/missing_values_test.csv"

    def test_missing_values_using_int32option_datatype(self):
        schema = [('a', ta.int32),('b', ta.float32option),('c', ta.float32option),('d',ta.vector(2))]
        csv = ta.CsvFile(self.csv_path,schema=schema,delimiter=":")


if __name__ == "__main__":
    unittest.main()
