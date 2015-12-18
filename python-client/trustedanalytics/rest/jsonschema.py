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
Json-Schema definitions and interactions
"""
import logging
logger = logging.getLogger(__name__)

import json

from trustedanalytics.meta.command import CommandDefinition, Parameter, ReturnInfo, Doc, ApiVersion
from trustedanalytics.meta.names import get_type_name
from trustedanalytics.core.atktypes import *

__all__ = ['get_command_def']


# See http://json-schema.org/documentation.html
# And source_code/engine/interfaces/src/main/scala/com/trustedanalytics/trustedanalytics/schema/JsonSchema.scala

json_type_id_to_data_type  = {
    "atk:int32": int32,
    "atk:int64": int64,
    "atk:float32": float32,
    "atk:float64": float64,
    "atk:bool": bool,
    "atk:vector": vector,
    "atk:datetime": datetime
}

_unspecified = object()


def get_data_type_from_json_schema_using_id(json_schema, default=_unspecified):
    try:
        if json_schema['id'].startswith("atk:vector"):
            return vector.get_from_string(json_schema['type'])
        return json_type_id_to_data_type[json_schema['id']]
    except:
        if default is _unspecified:
            raise ValueError("Unsupported JSON string for data type: %s" % json_schema)
        return default



# todo - this stuff needs big rework
# Need 1. the name of the type for documentation  frame-vertex --> VertexFrame, bool --> bool
#      2. the constructor for the type if it is in return value 'result' JSON
# atkschema.py - defines types to schema declaration
# atktypes.py - defines types for ATK API, including entities and result types, list and dictionary conversion (superset of atkschema)
#
# Going for stopgap for now, with a generic AtkReturnType to handle custom naming and constructors

class AtkReturnType(object):
    def __init__(self, name, constructor):
        self.name = name
        self.constructor = constructor

    @property
    def __name__(self):
        return self.name

    def __str__(self):
        return self.name

    def create(self, raw_value):
        return self.constructor(raw_value)

def _get_frame(uri_dict):
    from trustedanalytics import get_frame
    return get_frame(uri_dict['uri'])

def _get_graph(uri_dict):
    from trustedanalytics import get_graph
    return get_graph(uri_dict['uri'])

def _get_model(uri_dict):
    from trustedanalytics import get_model
    return get_model(uri_dict['uri'])

entity_schema_id_to_type = {'atk:frame': AtkReturnType("Frame", _get_frame),
                            'atk:graph': AtkReturnType("Graph", _get_graph),
                            'atk:model': AtkReturnType("Model", _get_model)}


def get_parameter_data_type(json_schema):
    """get data type from parameter json schema"""
    return _get_data_type(json_schema)

def get_return_data_type(json_schema):
    """get data type from return json schema"""

    # by convention, if returning dict with a single 'value' value, then we extract it
    if 'type' in json_schema and json_schema['type'] == 'object' and 'order' in json_schema and len(json_schema['order']) == 1 and json_schema['order'][0] == 'value':
        return_type = _get_data_type(json_schema['properties']['value'])
        type_name = get_type_name(return_type)

        def _return_single_value_from_dict(result):
            return result['value']
        return AtkReturnType(type_name, _return_single_value_from_dict)
    # otherwise, use regular get_data_type
    return _get_data_type(json_schema)

def _get_data_type(json_schema):
    """
    Returns Python data type for type found in the topmost element of the json_schema
    """
    try:
        data_type = None
        #print "json_schema=%s" % json.dumps(json_schema, indent=2)

        if 'type' in json_schema and json_schema['type'] == 'string' and 'format' in json_schema and json_schema['format'] == 'uri/entity':
            data_type = entity_schema_id_to_type[json_schema['id']]
        elif 'id' in json_schema:
            data_type = get_data_type_from_json_schema_using_id(json_schema, None)
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
                data_type = unicode
            elif t == 'array':
                data_type = list
            elif t == 'object':
                data_type = dict
            elif t == 'unit':
                data_type = unit

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
            data_type = get_parameter_data_type(properties)
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
    data_type = get_return_data_type(return_schema)
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
