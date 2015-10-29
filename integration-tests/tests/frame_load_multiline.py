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

class FrameMultiLineTests(unittest.TestCase):
    """
    Test loading multiline file formats (XML, JSON)

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        self.json = ta.JsonFile("/datasets/trailblazers.json")
        self.xml = ta.XmlFile("/datasets/foods.xml", "food")
        self.xml_root = ta.XmlFile("/datasets/foods.xml", "menu")

    def test_load_json(self):
        #covers escape double quote and nested json. They are in dataset
        frame = ta.Frame(self.json)
        self.assertEquals(frame.row_count, 15)
        self.assertEquals(frame.column_names, ["data_lines"])


    def test_load_xml(self):
        #covers xml comments
        frame = ta.Frame(self.xml)
        self.assertEquals(frame.row_count, 9)
        self.assertEquals(frame.column_names, ["data_lines"])


    def test_load_xml(self):
        #covers xml comments
        frame = ta.Frame(self.xml_root)
        self.assertEquals(frame.row_count, 1)
        self.assertEquals(frame.column_names, ["data_lines"])




if __name__ == "__main__":
    unittest.main()
