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
from trustedanalytics.core.files import *
from trustedanalytics.core.atktypes import *


class TestCsvFile(unittest.TestCase):

    def get_csv_and_schema(self):
        schema = [('A', int32), ('B', int64), ('C', float32)]
        return CsvFile("myfile.csv", schema), schema

    def test_csv_attributes(self):
        csv, schema = self.get_csv_and_schema()
        self.assertEqual(csv.file_name, "myfile.csv")
        self.assertEqual(csv.delimiter, ",")
        self.assertEqual(csv.skip_header_lines, 0)
        repr(csv)

    def test_field_names_and_types(self):
        csv, schema = self.get_csv_and_schema()
        field_names = csv.field_names
        field_types = csv.field_types
        self.assertEqual(len(schema), len(field_names))
        self.assertEqual(len(schema), len(field_types))
        for i in range(len(schema)):
            self.assertEqual(schema[i][0], field_names[i])
            self.assertEqual(schema[i][1], field_types[i])

    def test_bad_schema_bad_name(self):
        try:
            CsvFile("nice.csv", [(int, 'A'), (long, 'B')])
            self.fail()
        except ValueError as e:
            self.assertEqual('First item in CSV schema tuple must be a string', str(e))

    def test_bad_schema_bad_type(self):
        try:
            CsvFile("nice.csv", [('A', object), ('B', int)])
            self.fail()
        except ValueError as e:
            self.assertTrue(str(e).startswith('Second item in CSV schema tuple must be a supported type:'))

    def test_empty_schema(self):
        try:
            CsvFile("nice.csv", [])
            self.fail()
        except ValueError as e:
            self.assertEqual(str(e), 'schema must be non-empty list of tuples')

    def test_bad_file_name(self):
        try:
            CsvFile("", [('A', long),('B', str)])
            self.fail()
        except ValueError as e:
            self.assertTrue(str(e).startswith("file_name must be a non-empty string"))

    def test_empty_delimiter(self):
        try:
            CsvFile("nice.csv", [('A', long),('B', str)], "")
            self.fail()
        except Exception as e:
            self.assertTrue(str(e).startswith("delimiter must be a non-empty string"))

class TestJsonFile(unittest.TestCase):

    def test_json_file(self):
        file_name = 'new_kid.json'
        x = JsonFile(file_name)
        self.assertEqual(x.file_name, file_name)


class TestXmlFile(unittest.TestCase):

    def test_xml_file(self):
        file_name = 'ugly.xml'
        tag_name = 'YouKnow'
        y = XmlFile(file_name, tag_name)
        self.assertEqual(y.file_name, file_name)
        self.assertTrue("<%s>" % tag_name in y.start_tag)
        self.assertTrue("<%s " % tag_name in y.start_tag)

if __name__ == "__main__":
    unittest.main()
