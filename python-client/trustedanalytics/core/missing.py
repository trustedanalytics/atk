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
trustedanalytics frame options that specify how to treat missing values.
"""
import json


class MissingOptions(object):

    """
    Defines supported options that specify how to treat missing values
    """

    # specifies to ignore missing values
    ignore = {'missing': 'ignore'}

    # specifies an immediate value to replace in for missing values
    def imm(self, immediate_value):
        return {'missing': 'imm', 'value': immediate_value}

    def __repr__(self):
        return ", ".join([k for k in MissingOptions.__dict__.keys()
                          if isinstance(k, basestring) and not k.startswith("__")])

    def __contains__(self, item):
        return item in MissingOptions.__dict__.keys()

missing = MissingOptions()