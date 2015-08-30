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
Meta-programming - dynamically creating classes and functions
"""


import logging
logger = logging.getLogger('meta')

import sys
import inspect

from trustedanalytics.core.api import api_globals, api_status
from trustedanalytics.core.atktypes import unit
from trustedanalytics.meta.installpath import InstallPath
from trustedanalytics.meta.context import get_api_context_decorator
from trustedanalytics.meta.command import CommandDefinition, Parameter, ReturnInfo
from trustedanalytics.meta.spa import get_spa_docstring
from trustedanalytics.meta.names import name_to_private, class_name_to_entity_type, entity_type_to_baseclass_name, entity_type_to_collection_name, get_entity_constructor_command_full_name
from trustedanalytics.meta.clientside import mark_item_as_api, decorate_api_class


ATTR_COMMAND_INSTALLATION = "_command_installation"
ATTR_ENTITY_COLLECTION = "_entity_collection"
ATTR_INTERMEDIATE_CLASS = "_intermediate_class"
EXECUTE_COMMAND_FUNCTION_NAME = 'execute_command'


_installable_classes_store = {}  # global store, keyed on full install path, str -> cls   ex.  { 'frame:' : Frame }


class CommandInstallation(object):
    """
    Object that gets installed into a CommandInstallable class, as a private member, to hold command installation info.
    """

    def __init__(self, install_path, host_class_was_created):
        if not isinstance(install_path, InstallPath):
            install_path = InstallPath(install_path)
        self.install_path = install_path  # the path of which the installation's class is target
        self.host_class_was_created = host_class_was_created
        self.commands = []  # list of command definitions
        self.intermediates = {}  # property name --> intermediate class

    def add_property(self, parent_class, property_name, intermediate_class):
        if get_installation(parent_class) != self:
            raise RuntimeError("Internal error: installation and class type mismatch for class %s" % parent_class)
        if property_name not in parent_class.__dict__:   # don't use hasattr, because it would match an inherited prop
            prop = self._create_intermediate_property(property_name, intermediate_class, parent_class.__name__)
            setattr(parent_class, property_name, prop)
            self.intermediates[property_name] = intermediate_class

    @staticmethod
    def _create_intermediate_property(name, intermediate_class, parent_name):
        fget = CommandInstallation.get_fget(name)
        mark_item_as_api(fget)
        setattr(fget, ATTR_INTERMEDIATE_CLASS, intermediate_class)
        doc = CommandInstallation._get_canned_property_doc(name, intermediate_class.__name__)
        prop = property(fget=fget, doc=doc)
        fget.command = IntermediatePropertyCommandDefinition(name, prop, intermediate_class, parent_name)
        return prop

    @staticmethod
    def get_fget(name):
        private_name = name_to_private(name)

        def fget(self):
            return getattr(self, private_name)
        return fget

    @staticmethod
    def _get_canned_property_doc(name, class_name):
        return "Access to object's %s functionality (See :class:`~trustedanalytics.core.docstubs.%s`)" % (name, class_name)


def is_intermediate_property(item):
    """An intermediate property is a getter for an instance of an intermediate class, like graph.ml"""
    if isinstance(item, property):
        item = item.fget
    return hasattr(item, ATTR_INTERMEDIATE_CLASS)


class IntermediatePropertyCommandDefinition(CommandDefinition):
    """CommandDef for synthesized properties created for intermediate classes"""

    def __init__(self, name, prop, intermediate_class, parent_name):

        self.parent_name = parent_name

        function = prop.fget
        function.command = self  # make command def accessible from function, just like functions gen'd from server info

        json_schema = {}  # make empty, since this command didn't come from json, and there is no need to generate it
        full_name = self._generate_full_name(parent_name, name)

        params = []
        params.append(Parameter(name='self', data_type='object', use_self=True, optional=False, default=None, doc=None))
        return_info = ReturnInfo(intermediate_class, use_self=False, doc='%s object' % intermediate_class.__name__)

        maturity = function.maturity if hasattr(function, "maturity") else None

        super(IntermediatePropertyCommandDefinition, self).__init__(json_schema, full_name, params, return_info, is_property=True, doc=prop.__doc__, maturity=maturity)

        function.__doc__ = get_spa_docstring(self)

    @staticmethod
    def _generate_full_name(class_name, member_name):
        entity_type = class_name_to_entity_type(class_name)
        full_name = "%s/%s" % (entity_type, member_name)
        return full_name


class CommandInstallable(object):
    """
    Base class for objects which accept dynamically created members based on external info

    i.e. the class have commands installed dynamically

    Inheritors must...

    1.  Implement the following:
        instance attribute 'uri' : str
            identifies the instance to the server

    2.  Call CommandsInstallable.__init__(self) in its own __init__, IMMEDIATELY
    """

    def __init__(self, entity=None):
        logger.debug("Enter CommandsInstallable.__init__ from class %s" % self.__class__)
        # By convention, the entity instance is passed as the first arg for initializing intermediate classes.  This
        # is internal to the metaprogramming.  Standard entity class init is just called with only self.
        self._entity = entity if entity else self

        # Instantiate intermediate classes for the properties which scope installed commands.
        # To honor inheritance overriding, we must start at this class and work up to the most base class (not inc. CommandInstallable)
        class_lineage = inspect.getmro(self.__class__)
        index = class_lineage.index(CommandInstallable)
        for i in xrange(index):
            cls = class_lineage[i]
            if has_installation(cls):
                self._init_intermediate_classes(cls)

    def _init_intermediate_classes(self, installation_class):
            """Instantiates every intermediate class defined in the installation class and adds
            them as private members to the new_instance, such that the properties (already defined)
            have something to return"""
            installation = get_installation(installation_class)
            for name, cls in installation.intermediates.items():
                private_member_name = name_to_private(name)
                if private_member_name not in self.__dict__:
                    logger.debug("Instantiating intermediate class %s as %s", cls, private_member_name)
                    private_member_value = cls(self._entity)  # instantiate
                    logger.debug("Adding intermediate class instance as member %s", private_member_name)
                    setattr(self, private_member_name, private_member_value)


def is_class_command_installable(cls):
    return CommandInstallable in inspect.getmro(cls)


def get_default_init():
    """gets a default init method based on CommandInstallable"""

    def init(self, entity):
        CommandInstallable.__init__(self, entity)
    return init



def has_installation(cls):
    """tests if the given class type itself has a command installation object (and not inherited from a base class)"""
    return ATTR_COMMAND_INSTALLATION in cls.__dict__  # don't use hasattr


def get_installation(cls, *default):
    """
    returns the installation obj for given class type
    If default not provided, then an exception will be thrown if the class has not installation obj
    :rtype: CommandInstallation
    """
    try:
        return cls.__dict__[ATTR_COMMAND_INSTALLATION]  # don't use getattr
    except KeyError:
        if len(default) > 0:
            return default[0]
        raise AttributeError("Class %s does not have a command installation object" % cls)


def set_installation(cls, installation):
    """makes the installation object a member of the class type"""
    setattr(cls, ATTR_COMMAND_INSTALLATION, installation)
    _installable_classes_store[installation.install_path.full] = cls  # update store


def has_entity_collection(item):
    """the entity collection name for an item"""
    return hasattr(item, ATTR_ENTITY_COLLECTION)


def get_entity_collection(item, *default):
    """returns the entity collection name of given item"""
    try:
        return getattr(item, ATTR_ENTITY_COLLECTION)
    except AttributeError:
        if len(default) > 0:
            return default[0]
        raise AttributeError("Item %s does not have entity collection metadata" % item)


def set_entity_collection(item, collection_name):
    """the entity collection name for an item"""
    setattr(item, ATTR_ENTITY_COLLECTION, collection_name)


def get_installable_classes():
    return _installable_classes_store.values()


def get_class_from_store(install_path):
    """tries to find a class for the install_path in the store (or global API), returns None if not found"""
    cls = _installable_classes_store.get(install_path.full, None)
    if not cls:
        # see if cls is in the global API already and just needs to be added to the store by adding an installation obj
        class_name, baseclass_name = install_path.get_class_and_baseclass_names()
        for item in api_globals:
            if inspect.isclass(item) and item.__name__ == class_name:
                if not is_class_command_installable(item):
                    raise RuntimeError("Global class %s does not inherit %s, unable to install command with path '%s'" %
                                       (class_name, CommandInstallable.__name__, install_path.full))
                set_installation(item, CommandInstallation(install_path, host_class_was_created=False))
                return item
    return cls


def get_entity_class_from_store(entity_type):
    return get_class_from_store(InstallPath(entity_type))


def get_or_create_class(install_path):
    return get_class_from_store(install_path) or create_classes(install_path)


def get_intermediate_class(property_name, from_class):
    installation = get_installation(from_class, None)
    if installation:
        intermediate_class = installation.intermediates.get(property_name, None)
        if intermediate_class:
            return intermediate_class
    # keep looking...
    for cls in inspect.getmro(from_class)[1:]:
        installation = get_installation(cls, None)
        if installation:
            intermediate_class = installation.intermediates.get(property_name, None)
            if intermediate_class:
                # Need to make a new intermediate class for 'this' installation
                return intermediate_class
    return None


def _create_class(install_path, doc=None):
    """helper method which creates a single class for the given install path"""
    new_class_name, baseclass_name = install_path.get_class_and_baseclass_names()
    baseclass_install_path = install_path.baseclass_install_path
    #print "baseclass_install_path=%s" % baseclass_install_path
    if baseclass_name == CommandInstallable.__name__ or baseclass_install_path == install_path:
        baseclass = CommandInstallable
    else:
        baseclass = get_or_create_class(baseclass_install_path)
    new_class = _create_class_type(new_class_name,
                                   baseclass,
                                   doc=str(doc) or install_path.get_generic_doc_str(),
                                   init=get_class_init_from_path(install_path))
    set_installation(new_class, CommandInstallation(install_path, host_class_was_created=True))
    return new_class


def create_classes(install_path):
    """creates all the necessary classes to enable the given install path"""
    new_class = None
    parent_class = None
    for path in install_path.gen_composite_install_paths:
        path_class = get_class_from_store(path)
        if not path_class:
            new_class = _create_class(path)
            if parent_class is not None:
                get_installation(parent_class).add_property(parent_class, path.property_name, new_class)
            path_class = new_class
        parent_class = path_class
    if new_class is None:
        raise RuntimeError("Internal Error: algo was not expecting new_class to be None")
    return new_class


def create_entity_class(command_def):
    if not command_def.is_constructor:
        raise RuntimeError("Internal Error: algo was not a constructor command_def")
    new_class = _create_class(command_def.install_path, command_def.doc)
    decorate_api_class(new_class)


def _create_class_type(new_class_name, baseclass, doc, init=None):
    """Dynamically create a class type with the given name and namespace_obj"""
    if logger.level == logging.DEBUG:
        logger.debug("_create_class_type(new_class_name='%s', baseclass=%s, doc='%s', init=%s)",
                     new_class_name,
                     baseclass,
                     "None" if doc is None else "%s..." % doc[:12],
                     init)
    new_class = type(str(new_class_name),
                     (baseclass,),
                     {'__init__': init or get_default_init(),
                      '__doc__': doc,
                      '__module__': api_status.__module__})
    # assign to its module, and to globals
    # http://stackoverflow.com/questions/13624603/python-how-to-register-dynamic-class-in-module
    setattr(sys.modules[new_class.__module__], new_class.__name__, new_class)
    globals()[new_class.__name__] = new_class
    new_class._is_api = True
    return new_class

    #  Example: given commands "graph:titan/ml/pagerank" and "graph/ml/graphx_pagerank"  and class structure:
    #
    #     CommandsInstallable
    #            |
    #       _BaseGraph
    #       /         \
    #    Graph       TitanGraph
    #
    #
    #  We need to create:
    #
    #     _BaseGraphMl defines "graphx_pagerank"
    #       /         \
    #    GraphMl       TitanGraphMl defines "pagerank"
    #
    # such that
    #
    # t = TitanGraph()
    # t.ml.graphx_pagerank(...)   # works
    # t.ml.pagerank(...)          # works
    # g = Graph()
    # g.ml.graphx_pagerank(...)   # works
    # g.ml.pagerank(...)          # attribute 'pagerank' not found (desired behavior)


def get_class_init_from_path(install_path):
    if install_path.is_entity:
        # Means a class is being created for an entity and requires more information about the __init__ method
        # We return an __init__ that will throw an error when called.  It must be overwritten by a special plugin
        plugin_name = get_entity_constructor_command_full_name(install_path)
        msg = "Internal Error: server does not know how to construct entity type %s.  A command plugin " \
              "named \"%s\" must be registered." % (install_path.entity_type, plugin_name)

        def insufficient_init(self, *args, **kwargs):
            raise RuntimeError(msg)
        return insufficient_init

    return get_default_init()


def validate_arguments(arguments, parameters):
    """
    Returns validated and possibly re-cast arguments

    Use parameter definitions to make sure the arguments conform.  This function
    is closure over in the dynamically generated execute command function
    """
    validated = {}
    for (k, v) in arguments.items():
        try:
            parameter = [p for p in parameters if p.name == k][0]
        except IndexError:
            raise ValueError("No parameter named '%s'" % k)
        if hasattr(v, "uri"):
            v = v.uri
        validated[k] = v
        if parameter.data_type is list:
            if v is not None and (isinstance(v, basestring) or not hasattr(v, '__iter__')):
                validated[k] = [v]
    return validated


def _identity(value):
    return value


def _return_none(value):
    return None


def get_result_processor(command_def):
    from trustedanalytics.meta.results import get_postprocessor
    postprocessor = get_postprocessor(command_def.full_name)
    if postprocessor:
        return postprocessor
    if command_def.is_constructor:  # constructors always return raw JSON
        return _identity
    if command_def.return_info.data_type is unit:
        return _return_none
    if hasattr(command_def.return_info.data_type, "create"):   # this should grab the entities
        return command_def.return_info.data_type.create
    return _identity


def create_execute_command_function(command_def, execute_command_function):
    """
    Creates the appropriate execute_command for the command_def by closing
    over the parameter info for validating the arguments during usage
    """
    parameters = command_def.parameters
    result_processor = get_result_processor(command_def)

    def execute_command(_command_name, _selfish, **kwargs):
        arguments = validate_arguments(kwargs, parameters)
        result = execute_command_function(_command_name, _selfish, **arguments)
        return result_processor(result)
    return execute_command


def get_self_argument_text():
    """Produces the text for argument to use for self in a command call"""
    return "self._entity.uri"


def get_function_kwargs(command_def):
    return ", ".join(["%s=%s" % (p.name, p.name if not p.use_self else get_self_argument_text())
                      for p in command_def.parameters])


def get_function_text(command_def, body_text='return None', decorator_text=''):
    """Produces python code text for a command to be inserted into python modules"""

    args_text=command_def.get_function_args_text()
    if command_def.is_constructor:
        if args_text:
            args_text += ", _info=None"
        else:
            args_text = "_info=None"
    elif command_def.is_property:
        decorator_text = "@property\n" + decorator_text
    return '''{decorator}
def {name}({args_text}):
    """
{doc}
    """
    {body_text}
'''.format(decorator=decorator_text,
           name=command_def.name,
           args_text=args_text,
           doc=get_spa_docstring(command_def),
           body_text=body_text)


def get_call_execute_command_text(command_def):
    return "%s('%s', self, %s)" % (EXECUTE_COMMAND_FUNCTION_NAME,
                               command_def.full_name,
                               get_function_kwargs(command_def))


def get_repr_function(command_def):
    """Get repr function for a command def"""
    collection_name = entity_type_to_collection_name(command_def.entity_type)
    repr_func = default_repr

    def _repr(self):
        return repr_func(self, collection_name)
    return _repr


def default_repr(self, collection_name):
    """Default __repr__ for a synthesized class"""
    entity = type(self).__name__
    try:
        from trustedanalytics.rest.atkserver import server
        uri = server.create_full_uri(self.uri)
        response = server.get(uri).json()
        name = response.get('name', None)
        if name:
            details = ' "%s"' % response['name']
        else:
            details = ' <unnamed@%s>' % response['uri']
    except:
        raise
        #details = " (Unable to collect details from server)"
    return entity + details


def _get_init_body_text(command_def):
    """Gets text for the body of an init function for a constructor command_def"""
    return '''
    self.uri = None
    base_class.__init__(self)
    if {info} is None:
        {info} = {call_execute_create}
    self.uri = {info}['uri']
    # initialize_from_info(self, {info})  todo: implement
    '''.format(info='_info', call_execute_create=get_call_execute_command_text(command_def))


def _compile_function(func_name, func_text, dependencies):
    func_code = compile(func_text, '<string>', "exec")
    func_globals = {}
    eval(func_code, dependencies, func_globals)
    return func_globals[func_name]


def create_function(loadable_class, command_def, execute_command_function=None):
    """Creates the function which will appropriately call execute_command for this command"""
    execute_command = create_execute_command_function(command_def, execute_command_function)
    api_decorator = get_api_context_decorator(logging.getLogger(loadable_class.__module__))
    if command_def.is_constructor:
        func_text = get_function_text(command_def, body_text=_get_init_body_text(command_def), decorator_text='@api')
        #print "func_text for %s = %s" % (command_def.full_name, func_text)
        dependencies = {'api': api_decorator, 'base_class': _installable_classes_store.get(entity_type_to_baseclass_name(command_def.install_path.full), CommandInstallable), EXECUTE_COMMAND_FUNCTION_NAME: execute_command}
    else:
        func_text = get_function_text(command_def, body_text='return ' + get_call_execute_command_text(command_def), decorator_text='@api')
        dependencies = {'api': api_decorator,  EXECUTE_COMMAND_FUNCTION_NAME: execute_command}
    try:
        function = _compile_function(command_def.name, func_text, dependencies)
    except:
        sys.stderr.write("Metaprogramming problem compiling %s for class %s in code: %s" %
                         (command_def.full_name, loadable_class.__name__, func_text))
        raise
    function.command = command_def
    function.__doc__ = get_spa_docstring(command_def)
    return function
