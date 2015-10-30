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

from trustedanalytics.meta.names import entity_type_to_class_name, entity_type_to_baseclass_name, entity_type_to_entity_basetype


class InstallPath(object):
    """Indicates where an installation is located, includes properties and methods"""

    def __init__(self, path=''):
        self.full = path

        parts = path.split('/')
        self.entity_type = parts[0]
        self.entity_basetype = entity_type_to_entity_basetype(self.entity_type)
        self._intermediate_names = [name for name in (parts[1:] if len(parts) > 1 else []) if name]
        #print repr(self)

    def __repr__(self):
        return "%s" % self.__dict__

    @property
    def is_entity(self):
        return len(self._intermediate_names) == 0

    @property
    def property_name(self):
        """the last intermediate name --what would usually be the property name in the parent class"""
        try:
            return self._intermediate_names[-1]
        except IndexError:
            return None

    @property
    def entity_collection_name(self):
        """Name of the entity colelction, usually a pluralization of the entity_basetype.  ex. frame -> 'frames'"""
        return self.entity_basetype + 's'  # works until we get an entity name with non standard pluralization

    @property
    def gen_composite_install_paths(self):
        """generator for all the install paths that lead up to the full install path"""
        path = self.entity_type
        yield InstallPath(path)
        for i in self._intermediate_names:
            path = path + '/' + i
            yield InstallPath(path)

    @property
    def baseclass_install_path(self):
        path = '/'.join([self.entity_basetype] + self._intermediate_names)
        if path == self.full:
            return None
        return InstallPath(path)

    def get_class_and_baseclass_names(self):
        """Returns both the name of the Python class for this install path and the name of its base class"""
        suffix = ''.join([n[0].upper() + n[1:] for n in self._intermediate_names])
        class_prefix = entity_type_to_class_name(self.entity_type) + suffix
        baseclass_prefix = entity_type_to_baseclass_name(self.entity_type)
        if baseclass_prefix != "CommandInstallable":
            baseclass_prefix += suffix
        return class_prefix, baseclass_prefix

    def __str__(self):
        return self.full

    def get_generic_doc_str(self):
        """Creates a generic doc string based solely on the install path's full string"""
        class_name = entity_type_to_class_name(self.entity_type)
        if self.is_entity:
            return "Entity %s" % class_name
        return "Provides functionality scope for entity %s" % class_name
