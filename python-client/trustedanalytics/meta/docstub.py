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
Library for creating the docstub files, for Static Program Analysis (SPA)
"""

import inspect
import datetime
from decorator import decorator
import logging
logger = logging.getLogger('meta')

from trustedanalytics.meta.names import indent, get_type_name
from trustedanalytics.meta.clientside import DocStubCalledError
from trustedanalytics.meta.spa import get_spa_docstring
from trustedanalytics.meta.metaprog import CommandInstallable, CommandInstallation, get_installation


ATTR_DOC_STUB = '_doc_stub'


def doc_stub(item):
    """Doc stub decorator"""
    if not inspect.isclass(item):
        item = decorator(_doc_stub, item)
    setattr(item, ATTR_DOC_STUB, item.__name__)
    return item


def is_doc_stub(attr):
    if isinstance(attr, property):
        attr = attr.fget
    elif not hasattr(attr, '__call__'):
        attr = None
    return attr and hasattr(attr, ATTR_DOC_STUB)


def delete_docstubs():
    """
    Deletes all the doc_stub functions from all classes in docstubs.py
    """
    def _get_module_items(module):
        # created this method so IJ wouldn't complain about types below
        if module:
            return module.__dict__.values()
        raise RuntimeError("Internal error, no docstub module")

    def _delete_docstubs(docstubs):
        import trustedanalytics as ta
        items = _get_module_items(docstubs)
        for item in items:
            if inspect.isclass(item):
                victims = [k for k, v in item.__dict__.iteritems() if is_doc_stub(v)]
                logger.debug("deleting docstubs from %s: %s", item, victims)
                for victim in victims:
                    delattr(item, victim)
                if hasattr(item, ATTR_DOC_STUB) and hasattr(ta, item.__name__):
                    # print "Deleting %s from ta" % item.__name__
                    delattr(ta, item.__name__)

    try:
        import trustedanalytics.core.docstubs1 as docstubs1
    except Exception:
        logger.info("No docstubs1.py found, nothing to delete")
    else:
        _delete_docstubs(docstubs1)

    try:
        import trustedanalytics.core.docstubs2 as docstubs2
    except Exception:
        logger.info("No docstubs2.py found, nothing to delete")
    else:
        _delete_docstubs(docstubs2)


class DocStubsImport(object):
    """Methods for handling import of core/docstubs*.py from core/*.py files"""

    @staticmethod
    def success(module_logger, class_names_str):
        module_logger.info("Doc stubs inherited from docstubs.py for %s" % class_names_str)
        import os
        if os.getenv('TRUSTEDANALYTICS_BUILD_API_DOCS', False):
            raise RuntimeError("Doc stubs were inherited during build.  This probably means"
                               "the previous docstubs .py and/or .pyc files were not deleted")

    @staticmethod
    def failure(module_logger, class_names_str, e):
        msg = "Unable to inherit doc stubs from docstubs.py for %s: %s" % (class_names_str, e)
        module_logger.warn(msg)
        # import warnings
        # warnings.warn(msg, RuntimeWarning)
        return CommandInstallable


doc_stubs_import = DocStubsImport  # type alias


def get_doc_stub_property_text(name, class_name):
    return """@property
@{doc_stub}
def {name}(self):
    \"""
    {doc}
    \"""
    return {cls}()
    #raise RuntimeError("API error, trying to access a property written for documentation")
    """.format(doc_stub=doc_stub.__name__,
               name=name,
               doc=CommandInstallation._get_canned_property_doc(name, class_name),
               cls=class_name)


def get_doc_stub_init_text(command_def, override_rtype=None):
    args_text=command_def.get_function_args_text()
    return '''
def __init__({args_text}):
    """
    {doc}
    """
    raise {error}("{name}")
'''.format(args_text=args_text,
           doc=get_spa_docstring(command_def, override_rtype=override_rtype),
           error=DocStubCalledError.__name__,
           name=command_def.full_name)


def _doc_stub(function, *args, **kwargs):
    raise DocStubCalledError(function.__name__)


DOC_STUB_LOADABLE_CLASS_PREFIX = '_DocStubs'


def get_doc_stub_class_name(class_name):
    return DOC_STUB_LOADABLE_CLASS_PREFIX + class_name


def get_doc_stubs_class_text(class_name, baseclass_name, doc, members_text, decoration="@doc_stub"):
    """
    Produces code text for a loadable class definition
    """
    return '''
{decoration}
class {name}({baseclass}):
    """
{doc}
    """

{members}
'''.format(decoration=decoration, name=class_name, baseclass=baseclass_name, doc=indent(doc), members=members_text)


ATTR_DOC_STUB_TEXT = '_doc_stub_text'  # attribute for a function to hold on to its own doc stub text


def _has_doc_stub_text(item):
    return hasattr(item, ATTR_DOC_STUB_TEXT)


def _get_doc_stub_text(item):
    return getattr(item, ATTR_DOC_STUB_TEXT)


def _set_doc_stub_text(item, text):
    setattr(item, ATTR_DOC_STUB_TEXT, text)


def get_doc_stub_globals_text(module):
    doc_stub_all = []
    lines = []
    return_types = set()
    for key, value in sorted(module.__dict__.items()):
        if _has_doc_stub_text(value):
            doc_stub_text = _get_doc_stub_text(value)
            if doc_stub_text:
                doc_stub_all.append(key)
                lines.append(doc_stub_text)
                if hasattr(value, 'command'):
                    return_type = value.command.get_return_type()
                    if inspect.isclass(return_type):
                        return_types.add(return_type)
    for return_type in return_types:
        module_path = return_type.__module__
        lines.insert(0, "from %s import %s" % (module_path, get_type_name(return_type)))
    return '\n\n\n'.join(lines) if lines else '', doc_stub_all


DOCSTUB_FILE_IMPORT_NAMES = [doc_stub.__name__, DocStubCalledError.__name__]


def get_file_header_text():
    return """#
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

# Auto-generated file for API static documentation stubs ({timestamp})
#
# **DO NOT EDIT**

from {module} import {objects}

""".format(timestamp=datetime.datetime.now().isoformat(),
           module=__name__,
           objects=", ".join(DOCSTUB_FILE_IMPORT_NAMES))


def get_file_footer_text():
    """removes the imports brought in by the header from the namespace"""
    return "\n".join(["del %s" % name for name in DOCSTUB_FILE_IMPORT_NAMES])


def get_doc_stub_modules_text(class_to_member_text_dict, import_return_types):
    """creates spa text for two different modules, returning the content in a tuple"""

    # The first module contains dependencies for the entity classes that are 'hard-coded' in the python API,
    # like Frame, Graph...  They need things like  _DocStubsFrame, or GraphMl to be defined first.
    # The second module contains entity classes that are created by meta-programming, like LdaModel, *Model,
    # which may depend on the 'hard-coded' python API.  The second modules also contains any global methods,
    # like get_frame_names, which depend on objects like Frame being already defined.
    module1_lines = [get_file_header_text()]

    module2_lines = []
    module2_all = []  # holds the names which should be in module2's __all__ for import *

    classes = sorted([(k, v) for k, v in class_to_member_text_dict.items()], key=lambda kvp: kvp[0].__name__)
    for cls, members_info in classes:
        logger.info("Processing %s for doc stubs", cls)
        names, text = zip(*members_info)
        installation = get_installation(cls, None)
        if installation:
            class_name, baseclass_name = installation.install_path.get_class_and_baseclass_names()
            if class_name != cls.__name__:
                raise RuntimeError("Internal Error: class name mismatch generating docstubs (%s != %s)" % (class_name, cls.__name__))
            if installation.host_class_was_created and installation.install_path.is_entity:
                lines = module2_lines
                module2_all.append(class_name)
            else:
                if not installation.host_class_was_created:
                    class_name = get_doc_stub_class_name(class_name)
                lines = module1_lines

            lines.append(get_doc_stubs_class_text(class_name,
                                                 "object",  # no inheritance for docstubs, just define all explicitly
                                                 "Auto-generated to contain doc stubs for static program analysis",
                                                 indent("\n\n".join(text))))
        elif cls.__name__ == "trustedanalytics":
            module2_lines.extend(list(text))
            module2_all.extend(list(names))

    module2_lines.insert(0, '\n__all__ = ["%s"]' % '", "'.join(module2_all))

    # Need to import any return type to enable SPA, like for get_frame, we need Frame
    for t in import_return_types:
        if test_import(t):
            module2_lines.insert(0, "from trustedanalytics import %s" % t)

    module2_lines.insert(0, get_file_header_text())

    # remove doc_stub from namespace
    module1_lines.append(get_file_footer_text())
    module2_lines.append("\ndel doc_stub")

    return '\n'.join(module1_lines), '\n'.join(module2_lines)


def test_import(name):
    """Determines if the name is importable from main module"""
    try:
        import trustedanalytics as ta
        getattr(ia, name)
        return True
    except:
        return False
