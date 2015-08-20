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
import trustedanalytics.rest.jsonschema as js
import json


json_schema_cum_dist = """
    {
      "return_schema": {
        "required": [ "name", "status" ],
        "type": "object",
        "properties": {
          "status": {
            "minimum": -9.223372036854776e+18,
            "type": "number",
            "id": "atk:int64",
            "multiple_of": 1.0,
            "maximum": 9.223372036854776e+18
          },
          "error_frame_id": {
            "minimum": -9.223372036854776e+18,
            "type": "number",
            "id": "ia:long",
            "multiple_of": 1.0,
            "maximum": 9.223372036854776e+18
          },
          "name": {
            "type": "string"
          }
        },
        "order": [ "name", "error_frame_id", "status" ]
      },
    "name": "frame/cumulative_dist",
    "title": "Cumulative Distribution",
    "description": "Computes the cumulative distribution for a column and eats bags of Cheetos",
    "argument_schema": {
      "required": [
        "name",
        "sample_col"
      ],
      "type": "object",
      "properties": {
        "count_value": {
          "id": "atk:int64",
          "type": "number",
          "default": 0
        },
        "name": {
          "type": "string"
        },
        "dist_type": {
          "type": "string",
          "default": "super"
        },
        "sample_col": {
          "type": "string",
          "title": "The name of the column to sample"
        }
      },
      "order": [
        "name",
        "sample_col",
        "dist_type",
        "count_value"
      ]
    }
  }
"""

json_schema_join = """
{
    "return_schema": {
        "required": [ "name", "status" ],
        "type": "object",
        "properties": {
            "status": {
                "minimum": -9.223372036854776e+18,
                "type": "number",
                "id": "ia:long",
                "multiple_of": 1.0,
                "maximum": 9.223372036854776e+18
            },
            "error_frame_id": {
                "minimum": -9.223372036854776e+18,
                "type": "number",
                "id": "ia:long",
                "multiple_of": 1.0,
                "maximum": 9.223372036854776e+18
            },
            "name": {
                "type": "string"
            }
        },
        "order": [ "name", "error_frame_id", "status" ]
    },
    "name": "frame/join",
    "title": "Table join operation",
    "description": "Creates a new frame by joining two frames together",
    "argument_schema": {
        "required": [
            "columns",
            "left_on"
        ],
        "type": "object",
        "properties": {
            "columns": {
                "type": "string"
            },
            "left_on": {
                "type": "string"
            },
            "right_on": {
                "type": "string",
                "default": null
            },
            "how": {
                "type": "string",
                "default": "left",
                "title": "The name of the column to sample"
            }
        },
        "order": [
            "columns",
            "left_on",
            "right_on",
            "how"
        ]
    }
}
"""


class TestJsonSchema(unittest.TestCase):

    def cmd_repr(self, json_str):
        schema = json.loads(json_str)
        cmd = js.get_command_def(schema)
        print "#################################################################"
        print repr(cmd)

    def test1(self):
        # tests only running the code without error
        self.cmd_repr(json_schema_cum_dist)

    def test2(self):
        # tests only running the code without error
        self.cmd_repr(json_schema_join)


if __name__ == '__main__':
    unittest.main()
