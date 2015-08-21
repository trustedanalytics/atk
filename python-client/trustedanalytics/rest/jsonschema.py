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

"""
Json-Schema definitions and interactions
"""
import logging
logger = logging.getLogger(__name__)

import json

from trustedanalytics.meta.command import CommandDefinition, Parameter, ReturnInfo, Doc, ApiVersion
from trustedanalytics.core.atktypes import *
from trustedanalytics.core.frame import Frame
from trustedanalytics.core.graph import Graph

__all__ = ['get_command_def']


# See http://json-schema.org/documentation.html
# And source_code/engine/interfaces/src/main/scala/com/trustedanalytics/trustedanalytics/schema/JsonSchema.scala

json_type_id_to_data_type  = {
    "atk:int": int32,
    "atk:long": int64,
    "atk:float": float32,
    "atk:double": float64,
    "atk:bool": bool,
}

_unspecified = object()


def get_data_type_from_json_type_id(json_type_str, default=_unspecified):
    try:
        if json_type_str.startswith("atk:vector"):
            return vector.get_from_string(json_type_str[3:])
        return json_type_id_to_data_type[json_type_str]
    except:
        if default is _unspecified:
            raise ValueError("Unsupported JSON string for data type: %s" % json_type_str)
        return default


json_str_formats_to_data_type = {
    "uri/atk-frame": Frame,
    "uri/atk-graph": Graph,
}


def get_data_type(json_schema):
    """
    Returns Python data type for type found in the topmost element of the json_schema
    """
    try:
        data_type = None
        # first try from id
        if 'id' in json_schema:
            data_type = get_data_type_from_json_type_id(json_schema['id'], None)
        # next try from type
        if not data_type:
            if 'type' not in json_schema:
                return None
            t = json_schema['type']
            if t == 'null':
                return None  # exit
            if t == 'boolean':
                return bool
            if t == 'string':
                if 'format' in json_schema:
                    data_type = json_str_formats_to_data_type.get(json_schema['format'], unicode)
                else:
                    data_type = unicode
            elif t == 'array':
                data_type = list
            elif t == 'object':
                data_type = dict  # use dict for now, TODO - add complex type support

        if not data_type:
            #data_type =  dict  # use dict for now, TODO - add complex type support
            raise ValueError("Could not determine data type from server information:\n  %s" %
                             ("\n  ".join(["%s-%s" % (k, v) for k, v in json_schema.iteritems()])))
        return data_type
    except:
        log_schema_error(json_schema)
        raise


def log_schema_error(json_schema):
    if json_schema is None:
        schema_str = "(json_schema is None!)"
    else:
        try:
            schema_str = json.dumps(json_schema, indent=2)
        except:
            schema_str = "(Unable to dump schema!)"
    logger.error("Error in json_schema:\n%s" % schema_str)


def get_parameter_description(json_schema):
    return json_schema.get('description', '')


def get_return_description(json_schema):
    return json_schema.get('description', '')


def get_doc(json_schema):
    doc = json_schema.get('doc', {})
    title = doc.get('title', '<Missing Doc>').strip()
    description = (doc.get('description', '') or '').lstrip()
    examples = (doc.get('examples', {}) or {}).get('python', '').lstrip()
    return Doc(title, description, examples)


def get_parameters(argument_schema):
    """Builds list of Parameter tuples as represented by the 'argument_schema'"""
    # Note - using the common convention that "parameters" are the variables in function definitions
    # and arguments are the values being passed in.  'argument_schema' is used in the rest API however.
    parameters = []
    if 'order' in argument_schema:
        for name in argument_schema['order']:
            properties = argument_schema['properties'][name]
            data_type = get_data_type(properties)
            use_self = properties.get('self', False)
            optional = name not in argument_schema['required']
            default = properties.get('default_value', None)
            doc = get_parameter_description(properties)
            parameters.append(Parameter(name, data_type, use_self, optional, default, doc))
    return parameters


def get_return_info(return_schema):
    """Returns a Return tuple according to the return_schema"""
    # Get the definition of what happens with the return  --TODO, enhance for Complex Types, etc...
    # 1. return Simple/Primitive Type
    # 2. return Frame or Graph reference
    # 3. return Complex Type
    # 4. return None  (no return value)
    data_type = get_data_type(return_schema)
    use_self = return_schema.get('self', False)
    #if use_self and data_type not in [Frame, Graph]:
    #    raise TypeError("Error loading commands: use_self is True, but data_type is %s" % data_type)
    doc = get_return_description(return_schema)
    return ReturnInfo(data_type, use_self, doc)


def get_api_version(json_schema):   # TODO - this is first-cut, needs reqs+review
    """Returns a Version object or None"""
    v = json_schema.get('apiversion', {})
    return ApiVersion(v.get('added', None), v.get('changed', None), v.get('deprecated', None), v.get('description', None))


def get_command_def(json_schema):
    """Returns a CommandDefinition obj according to the json schema"""
    full_name = json_schema['name']
    parameters = get_parameters(json_schema['argument_schema'])
    return_info = get_return_info(json_schema['return_schema'])
    api_version = get_api_version(json_schema)
    maturity = json_schema.get('maturity', None)
    doc = get_doc(json_schema)
    return CommandDefinition(json_schema, full_name, parameters, return_info, is_property=False, doc=doc, maturity=maturity, api_version=api_version)
