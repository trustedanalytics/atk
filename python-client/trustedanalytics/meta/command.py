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
Command Definition and subservient objects
"""

import logging
logger = logging.getLogger('meta')
from collections import namedtuple

from trustedanalytics.meta.installpath import InstallPath
from trustedanalytics.meta.doc import Doc
from trustedanalytics.meta.names import default_value_to_str, is_entity_constructor_command_name


Parameter = namedtuple("Parameter", ['name', 'data_type', 'use_self', 'optional', 'default', 'doc'])

ReturnInfo = namedtuple("Returns", ['data_type', 'use_self', 'doc'])

ApiVersion = namedtuple("ApiVersion", ['added', 'changed', 'deprecated', 'doc'])



class CommandDefinition(object):
    """Defines a Command"""

    def __init__(self, json_schema, full_name, parameters=None, return_info=None, is_property=False, doc=None, maturity=None, api_version=None):
        self.json_schema = json_schema
        self.full_name = full_name
        parts = self.full_name.split('/')
        self.entity_type = parts[0]
        if not self.entity_type:
            raise ValueError("Invalid empty entity_type, expected non-empty string")
        self.intermediates = tuple(parts[1:-1])
        self.name = parts[-1]
        self.install_path = InstallPath(full_name[:-(len(self.name)+1)])
        self.parameters = parameters if parameters else []
        self.return_info = return_info
        self.is_property = is_property
        self.maturity = maturity
        self.version = api_version
        self._doc = None  # handle type conversion in the setter, next line
        self.doc = doc

    @property
    def doc(self):
        return self._doc

    @doc.setter
    def doc(self, value):
        if value is None:
            self._doc = Doc()
        elif isinstance(value, basestring):
            self._doc = Doc.get_from_str(value)
        elif isinstance(value, Doc):
            self._doc = value
        else:
            raise TypeError("Received bad type %s for doc, expected type %s or string" % (type(value), Doc))

    @property
    def is_constructor(self):
        return is_entity_constructor_command_name(self.name) or self.name == "__init__"

    @property
    def function_name(self):
        return "__init__" if self.is_constructor else self.name

    @property
    def doc_name(self):
        return '.'.join(list(self.intermediates) + [self.function_name])

    def __repr__(self):
        return "\n".join([self.full_name,
                          "\n".join([repr(p) for p in self.parameters]) if self.parameters else "<no parameters>",
                          repr(self.return_info),
                          repr(self.version),
                          "Doc" + repr(self.doc)])

    def get_return_type(self):
        return None if self.return_info is None else self.return_info.data_type

    def get_function_args_text(self):
        if self.parameters:
            return ", ".join(['self' if param.use_self else
                              param.name if not param.optional or param.name.startswith('*') else
                              "%s=%s" % (param.name, default_value_to_str(param.default))
                              for param in self.parameters])
        else:
            return ''
