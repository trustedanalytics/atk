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

"""
Conversion functions for data type to/from REST strings
"""
from trustedanalytics.core.atktypes import valid_data_types


def get_rest_str_from_data_type(data_type):
    """Returns the REST string representation for the data type"""
    return valid_data_types.to_string(data_type)  # REST accepts all the Python data types


def get_data_type_from_rest_str(rest_str):
    """Returns the data type for REST string representation"""
    if rest_str == 'string':  # string is a supported REST type; if more aliases crop up, make a table in this module
        rest_str = 'unicode'
    return valid_data_types.get_from_string(rest_str)
