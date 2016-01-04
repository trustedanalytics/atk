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
Install API methods, and other API utils
"""

import inspect
import json
import warnings
import logging
logger = logging.getLogger('meta')

from trustedanalytics.core.api import api_status, api_globals
from trustedanalytics.meta.names import get_type_name
from trustedanalytics.meta.docstub import delete_docstubs, is_doc_stub
from trustedanalytics.meta.clientside import client_commands, clear_clientside_api_stubs, is_api, ClientCommandDefinition
from trustedanalytics.meta.mute import muted_commands
import trustedanalytics.meta.metaprog as metaprog


def handle_client_server_version_mismatch(client_version, server_version):
    """
    Handles a client/server version mismatch.(disable to this check

    To turn this client/server version check OFF, change the value of 'version' to
    be None before connecting:

    import trustedanalytics as ta
    ta.version = None
    ta.connect()
    """
    msg = """
------------------------------------------------------------------------------
WARNING - Client version '%s' does not match server version '%s'.
------------------------------------------------------------------------------""" % (client_version, server_version),

    #raise RuntimeError(version_help_msg)
    with warnings.catch_warnings():
        warnings.simplefilter('default')  # make it so Python 2.7 will still report this warning
        warnings.warn(msg, RuntimeWarning, stacklevel=2)


def version_match(client_id, server_id):
    """Compares build IDs of the client and server"""
    return client_id == server_id   # simple equality check, could make more sophisticated...


def get_version(server_response):
    """Extract and returns server build ID from server response"""
    # if trustedanalytics.version is not None (i.e. the client version is set),
    # then a check will be made against the build_server_id
    try:
        from trustedanalytics import version as client_version
    except:
        client_version = None
    try:
        server_version = server_response.headers['version']
    except KeyError:
        server_version = "<Server response did not provide a version>"

    if client_version and not version_match(client_version, server_version):
        handle_client_server_version_mismatch(client_version, server_version)
    return server_version


def request_server_command_defs(server):
    logger.info("Requesting available commands from server")
    from trustedanalytics.core.errorhandle import IaError
    try:
        return server.get("/commands/definitions")
    except:
        import sys
        sys.stderr.write('Unable to connect to server %s\n' % server._get_base_uri())
        raise IaError(logger)


def download_server_details(server):
    """Ask server for details about itself: the build ID and the API command defs"""
    logger.info("Requesting available commands from server")
    from trustedanalytics.rest.jsonschema import get_command_def
    response = request_server_command_defs(server)
    server_version = get_version(response)
    commands_json_schema = response.json()
    # ensure the assignment to __commands_from_backend is the last line in this 'if' block before the fatal try:
    return server_version, [get_command_def(c) for c in commands_json_schema]


def install_client_commands():
    cleared_classes = set()
    for class_name, command_def in client_commands:

        # what to do with global methods???

        # validation
        cls = None
        if class_name:
            try:
                cls = [c for c in api_globals if inspect.isclass(c) and c.__name__ == class_name][0]
            except IndexError:
                #raise RuntimeError("Internal Error: @api decoration cannot resolve class name %s for function %s" % (class_name, command_def.name))
                pass
        if cls:
            cls_with_installation = metaprog.get_class_from_store(metaprog.InstallPath(command_def.entity_type))
            if cls_with_installation is not cls:
                raise RuntimeError("Internal Error: @api decoration resolved with mismatched classes for %s" % class_name)
            if cls not in cleared_classes:
                clear_clientside_api_stubs(cls)
                cleared_classes.add(cls)
            installation = metaprog.get_installation(cls_with_installation)
            if command_def.entity_type != installation.install_path.full:
                raise RuntimeError("Internal Error: @api decoration resulted in different install paths '%s' and '%s' for function %s in class %s"
                                   % (command_def.entity_type, installation.install_path, command_def.name, class_name))

            if command_def.full_name not in muted_commands:
                installation.commands.append(command_def)
                setattr(cls, command_def.name, command_def.client_member)
                logger.debug("Installed client-side api function %s to class %s", command_def.name, cls)

        elif command_def.client_member not in api_globals:
            # global function
            api_globals.add(command_def.client_member)


def install_command_def(cls, command_def, execute_command_function):
    """Adds command dynamically to the loadable_class"""
    if command_def.full_name not in muted_commands:
        if is_command_name_installable(cls, command_def.name):
            check_loadable_class(cls, command_def)
            function = metaprog.create_function(cls, command_def, execute_command_function)
            function._is_api = True
            if command_def.is_constructor:
                cls.__init__ = function
                cls.__repr__ = metaprog.get_repr_function(command_def)
            else:
                setattr(cls, command_def.name, function)
            metaprog.get_installation(cls).commands.append(command_def)
            #print "%s <-- %s" % (cls.__name__, command_def.full_name)
            logger.debug("Installed api function %s to class %s", command_def.name, cls)


def handle_constructor_command_defs(constructor_command_defs):
    for d in sorted(constructor_command_defs, key=lambda x: len(x.full_name)):  # sort so base classes are created first
        metaprog.create_entity_class_from_constructor_command_def(d)


def is_command_name_installable(cls, command_name):
    # name doesn't already exist or exists as a doc_stub
    return not hasattr(cls, command_name) or is_doc_stub(getattr(cls, command_name))


def check_loadable_class(cls, command_def):

    error = None
    installation = metaprog.get_installation(cls)
    if not installation:
        error = "class %s is not prepared for command installation" % cls
    elif command_def.is_constructor and not installation.install_path.is_entity:
        raise RuntimeError("API load error: special method '%s' may only be defined on entity classes, not on entity "
                           "member classes (i.e. only one slash allowed)." % command_def.full_name)
    elif command_def.entity_type != installation.install_path.entity_type:
        error = "%s is not the class's accepted command entity_type: %s" \
                % (command_def.entity_type, installation.install_path.entity_type)
    if error:
        raise ValueError("API load error: Class %s cannot load command_def '%s'.\n%s"
                         % (cls.__name__, command_def.full_name, error))


def install_server_commands(command_defs):
    from trustedanalytics.rest.command import execute_command

    # Unfortunately we need special logic to handle command_defs which define constructors
    # for entity classes.  We must install the constructor command_defs first, such that the
    # appropriate entity classes get built knowing their docstring (because __doc__ on a type
    # is readonly in Python --the real problem), instead of being built generically as
    # dependencies of regular command_defs.
    handle_constructor_command_defs([d for d in command_defs if d.is_constructor])

    for command_def in command_defs:
        cls = metaprog.get_or_create_class(command_def.install_path)
        install_command_def(cls, command_def, execute_command)

    post_install_clean_up()


def post_install_clean_up():
    """Do some post-processing on the installation to clean things like property inheritance"""

    # Find all the intermediate properties that are using a base class and create an appropriate
    # inherited class that matches the inheriance of its parent class
    # Example:  say someone registers   "graph/colors/red"
    # We end up with  _BaseGraphColors, where _BaseGraph has a property 'colors' which points to it
    # Now, regular old Graph(_BaseGraph) did get any commands installed at colors specifically, so it inherits property 'color' which points to _BaseGraphColors
    # This makes things awkward because the Graph class installation doesn't know about the property color, since it wasn't installed there.
    # It becomes much more straightforward in Graph gets its own proeprty 'colors' (overriding inheritance) which points to a class GraphColors
    # So in this method, we look for this situation, now that installation is complete.
    installable_classes = metaprog.get_installable_classes()
    for cls in installable_classes:
        installation = metaprog.get_installation(cls)
        if installation:
            for name in dir(cls):
                member = getattr(cls, name)
                if metaprog.is_intermediate_property(member):
                    if name not in cls.__dict__:  # is inherited...
                        path =metaprog.InstallPath(installation.install_path.entity_type + '/' + name)
                        new_class = metaprog._create_class(path)  # create new, appropriate class to avoid inheritance
                        installation.add_property(cls, name, new_class)
                        metaprog.get_installation(new_class).keep_me = True  # mark its installation so it will survive the next for loop
            # Some added properties may access intermediate classes which did not end up
            # receiving any commands. They are empty and should not appear in the API.  We
            # will take a moment to delete them.  This approach seemed much better than writing
            # special logic to calculate fancy inheritance and awkwardly insert classes into
            # the hierarchy on-demand.
            for name, intermediate_cls in installation.intermediates.items():
                intermediate_installation = metaprog.get_installation(intermediate_cls)
                if not intermediate_installation.commands and not hasattr(intermediate_installation, "keep_me"):
                    delattr(cls, name)
                    del installation.intermediates[name]


def install_api(server):

    """
    Download API information from the server, once.

    After the API has been loaded, it cannot be changed or refreshed.  Also, server connection
    information cannot change.  User must restart Python in order to change connection info.

    Subsequent calls to this method invoke no action.
    """
    if not api_status.is_installed:
        server_version, server_commands = download_server_details(server)
        delete_docstubs()
        install_client_commands()  # first do the client-side specific processing
        install_server_commands(server_commands)
        from trustedanalytics import _refresh_api_namespace
        _refresh_api_namespace()
        api_status.declare_installed(server, server_version)


class FatalApiLoadError(RuntimeError):
    def __init__(self, e):
        self.details = str(e)
        RuntimeError.__init__(self, """
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Fatal error: installing the downloaded API information failed and has left the
client in a state of unknown compatibility with the server.

Restarting your python session is now required to use this package.

Details:%s
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""" % self.details)


def walk_api(cls, cls_function, attr_function, include_init=False):
    """
    Walks the API and executes the cls_function at every class and the attr_function at every attr

    Skip all private methods.  If __init__ should be included (special case), set include_init=True

    Either *_function arg can be None.  For example if you're only interesting in walking functions,
    set cls_function=None.
    """
    cls_func = cls_function
    attr_func = attr_function
    inc_init = include_init

    def walker(obj):
        for name in dir(obj):
            if not name.startswith("_") or (inc_init and name == "__init__"):
                a = getattr(obj, name)
                if inspect.isclass(a):
                    if is_api(a) and cls_func:
                        cls_func(a)
                    walker(a)
                else:
                    if isinstance(a, property):
                        intermediate_class = metaprog.get_intermediate_class(name, obj)
                        if intermediate_class:
                            walker(intermediate_class)
                        a = a.fget
                    if is_api(a) and attr_func:
                        attr_func(obj, a)
    walker(cls)


class ApiInfo(object):
    """Provides listing of Class objects and their command_defs"""

    def __init__(self, obj):
        """obj should be the trustedanalytics module, or some API object"""
        self.command_defs_by_class = {}
        self._build(obj)

    def _build(self, obj):
        self.command_defs_by_class.clear()
        info = self

        def collect(o, a):
            try:
                if not metaprog.is_intermediate_property(a):
                    info._add(o, a.command)
            except:
                pass

        walk_api(obj, None, collect, include_init=True)

    def _add(self, cls, command_def):
        if cls not in self.command_defs_by_class:
            self.command_defs_by_class[cls] = []
        self.command_defs_by_class[cls].append(command_def)

    @staticmethod
    def _get_side(command):
        return 'client' if isinstance(command, ClientCommandDefinition) else 'server'

    def get_command_defs(self):
        """Get all the command_defs from the API"""
        return set([c for commands in self.command_defs_by_class.values() for c in commands])

    def get_unique_command_names(self):
        return sorted([str(c.full_name) for c in self.get_command_defs()])

    def get_unique_command_names_with_side(self):
        """
        Gets tuple unique command full names, and a side indication.

        Side is 'client' if the api was defined 'clientside' or side is 'server' if the
        command was completely generated by metaprogramming from info strictly from the server
        If side is 'server', then the CommandDef full name may be used as is in the REST API.
        """
        return sorted([(str(c.full_name), self._get_side(c)) for c in self.get_command_defs()])

    def get_class_command_names(self):
        return sorted(set(["%s.%s" % (get_type_name(cls), str(c.name) if c.name != 'new' else '__init__')
                           for cls, commands in self.command_defs_by_class.items() for c in commands]))

    def get_big_tuples(self):
        """
        Returns a tuple of strings: (The Python <class>.<name>, Command Def full name, side)

        Side is 'client' if the api was defined 'clientside' or side is 'server' if the
        command was completely generated by metaprogramming from info strictly from the server
        If side is 'server', then the CommandDef full name may be used as is in the REST API.
        """
        return sorted(set([("%s.%s" % (get_type_name(cls), str(c.name) if c.name != 'new' else '__init__'),
                            str(c.full_name),
                            self._get_side(c))
                           for cls, commands in self.command_defs_by_class.items() for c in commands]))



    def __repr__(self):
        return "\n".join([str(t) for t in self.get_big_tuples()])


class ServerApiRaw(object):
    """Object to hold raw metadata JSON from the server concerning the commands API"""

    def __init__(self, server):
        """Fetches API command metadata from the given server"""
        self.server = server
        self.raw = request_server_command_defs(self.server).json()

    @property
    def count(self):
        """number of API commands presented by the server"""
        return len(self.raw)

    def get_command_meta_keys(self):
        """Gets all the keys in the json object which describes a command"""
        if len(self.raw) > 0:
            return self.raw[0].keys()
        raise RuntimeError("There are no commands from which to extract the keys.")

    def get_command_names(self, search=None):
        """Lists all the command names from the server, with option search string with wildcard"""
        if search:
            import re
            pattern = re.compile(search.replace("*", ".*?"))  # change to regex non-greedy wildcard
            return [x['name'] for x in self.get_command_meta(select="name") if pattern.search(x['name'])]

        return [x['name'] for x in self.get_command_meta(select="name")]

    def get_command_meta(self, command_name=None, where=None, select=None):
        """presents the command meta data as json, with optional filters"""
        if command_name is not None:
            if where is not None:
                raise ValueError("cannot set both 'command_name' and 'where'; use only 'where' in this situation.")

            if "*" in command_name:
                import re
                pattern = re.compile(command_name.replace("*", ".*?"))  # change to regex non-greedy wildcard

                def command_name_wild_where(command):
                    return pattern.search(command["name"])
                where = command_name_wild_where

            else:
                name = command_name

                def command_name_where(command):
                    return command["name"] == name
                where = command_name_where

        if isinstance(select, list) or isinstance(select, basestring):
            if isinstance(select, basestring):
                select = [select]
            keys = select
            if "name" not in keys:
                keys.append("name")

            def select_keys(command):
                try:
                    return dict([(key, command[key]) for key in keys])
                except KeyError as e:
                    try:
                        msg = e.message + "\n" + str(get_command_meta_keys())
                    except:
                        msg = e.message
                    raise KeyError(msg)
            select = select_keys

        return self._filter_raw(where=where, select=select)

    def dump_command_meta_to_file(self, file_name, command_name=None, where=None, select=None):
        """Gets command defs from server and dumps them to a file, as raw JSON"""
        commands = self.get_command_meta(command_name, where, select)
        with open(file_name, "w") as f:
            json.dump(commands, f, indent=2)

    def _filter_raw(self, where=None, select=None):
        """helper method to filter the raw json"""
        if not where:
            def w(command):
                return True
            where = w
        if not select:
            def s(command):
                return command
            select = s
        return [select(c) for c in self.raw if where(c)]

