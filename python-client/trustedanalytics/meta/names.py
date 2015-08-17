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
Text utility functions for names, entity_types, etc. and the indent method
"""

def indent(text, spaces=4):
    indentation = ' ' * spaces
    return "\n".join([indentation + line if line else line for line in text.split('\n')])


def default_value_to_str(value):
    """renders value to be printed in a function signature"""
    return value if value is None or type(value) not in [str, unicode] else "'%s'" % value


ENTITY_CONSTRUCTOR_COMMAND_RESERVED_NAME = "new"


def get_entity_constructor_command_full_name(install_path):
    return "%s/%s" % (install_path, ENTITY_CONSTRUCTOR_COMMAND_RESERVED_NAME)


def is_entity_constructor_command_name(name):
    return name == ENTITY_CONSTRUCTOR_COMMAND_RESERVED_NAME


def is_name_private(name):
    return name.startswith('_') and name != "__init__"


def name_to_private(name):
    """makes private version of the name"""
    return name if name.startswith('_') else '_' + name


def upper_first(s):
    """Makes first character uppercase"""
    return '' if not s else s[0].upper() + s[1:]


def lower_first(s):
    """Makes first character lowercase"""
    return '' if not s else s[0].lower() + s[1:]


def underscores_to_pascal(s):
    return '' if not s else ''.join([upper_first(s) for s in s.split('_')])


def pascal_to_underscores(s):
    return ''.join(["_%s" % c.lower() if c.isupper() else c for c in s])[1:]


def class_name_to_entity_type(class_name):
    if not class_name:
        raise ValueError("Invalid empty class_name, expected non-empty string")
    if class_name.startswith("_Base"):
        return pascal_to_underscores(class_name[5:])
    pieces = pascal_to_underscores(class_name).split('_')
    return "%s:%s" % (pieces[-1], '_'.join(pieces[:-1]))


def entity_type_to_class_name(entity_type):
    if not entity_type:
        raise ValueError("Invalid empty entity_type, expected non-empty string")
    parts = entity_type.split(':')
    term = underscores_to_pascal(parts[0])
    if len(parts) == 1:
        return "_Base" + term
    else:
        return underscores_to_pascal(parts[1]) + term


def entity_type_to_baseclass_name(entity_type):
    parts = entity_type.split(':')
    term = underscores_to_pascal(parts[0])
    if len(parts) == 1:
        from trustedanalytics.meta.metaprog import CommandInstallable  # todo: refactor, remove circ dep
        return CommandInstallable.__name__
    return "_Base" + term


def entity_type_to_entity_subtype(entity_type):
    split_index = entity_type.find(':')
    return '' if split_index < 1 else entity_type[split_index+1:]


def entity_type_to_entity_basetype(entity_type):
    split_index = entity_type.find(':')
    return entity_type if split_index < 1 else entity_type[:split_index]


def entity_type_to_collection_name(entity_type):
    return entity_type_to_entity_basetype(entity_type) + "s"


def get_type_name(data_type):
    if isinstance(data_type, basestring):
        return data_type
    return data_type.__name__ if data_type is not None else 'None'
