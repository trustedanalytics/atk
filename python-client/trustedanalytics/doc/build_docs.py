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
Script to generate all the collateral needed for documentation and Static Program Analysis (SPA)

Builds .rst files and folder structure for Python API HTML+PDF documentation

Builds .rst files and folder structure for REST API HTML+PDF documentation

Builds docstub*.py files for SPA
"""

# todo: break this script up a little, move to genrst.py and genspa.py

import shutil
import os
import errno
import sys
import glob
import logging
logger = logging.getLogger(__name__)


this_script_dir = os.path.dirname(os.path.abspath(__file__))
source_code_dir = os.path.join(os.path.join(os.path.join(this_script_dir, os.pardir), os.pardir), os.pardir)
doc_source_dir = os.path.join(source_code_dir, "doc/source")
python_api = "python_api"
rest_api = "rest_api"
dst_python_api_dir = os.path.join(doc_source_dir, python_api)
dst_rest_api_dir = os.path.join(doc_source_dir, rest_api)
dst_rest_api_commands_dir = os.path.join(dst_rest_api_dir, "v1/commands")
dst_docstubs_dir = os.path.join(source_code_dir, r'python-client/trustedanalytics/core')

# override the python path so that 'this' trustedanalytics package is used
sys.path.insert(0, os.path.join(source_code_dir, "python-client"))


spa_module1_file_name = os.path.join(dst_docstubs_dir, 'docstubs1.py')
spa_module2_file_name = os.path.join(dst_docstubs_dir, 'docstubs2.py')


def delete_existing_docstubs():
    for file_name in [spa_module1_file_name, spa_module2_file_name]:
        for existing_doc_file in glob.glob("%s*" % file_name):  # * on the end to get the .pyc as well
            print "Deleting existing docstub file: %s" % existing_doc_file
            os.remove(existing_doc_file)

# Delete any existing docstub.py files BEFORE importing ia
delete_existing_docstubs()

# Connect to running server
import trustedanalytics as ta
print "Using package: %s" % ta.__file__
ta.connect()


from trustedanalytics.meta.metaprog import get_installation, has_entity_collection, get_entity_collection, get_function_text
from trustedanalytics.meta.docstub import get_doc_stub_modules_text, get_doc_stub_init_text
from trustedanalytics.meta.names import upper_first, entity_type_to_class_name, indent, get_type_name, is_entity_constructor_command_name
from trustedanalytics.meta.installapi import download_server_details
from trustedanalytics.doc.pyrst import  get_command_def_rst, get_class_rst
from trustedanalytics.doc.restrst import get_command_def_rest_rst, get_commands_rest_index_content, get_command_rest_rst_file_name

COLLECTION_MARKER_FILE_NAME = ".collection"  # name of an empty file we create in a folder to mark it as a collection


def delete_folder(path):
    if os.path.exists(path):
        print "Deleting folder %s" % path
        shutil.rmtree(path)


def copy_template(src_template_name):
    """copies folder to folder of same name in the doc/source dir"""
    dst_path = os.path.join(doc_source_dir, src_template_name)
    print "Copying %s to %s" % (src_template_name, dst_path)
    shutil.copytree(src_template_name, dst_path)


def write_text_to_file(file_name, text):
    with open(file_name, 'w') as f:
        print "Writing file %s" % file_name
        if isinstance(text, list):
            f.writelines(text)
        else:
            f.write(text)


def write_dummy_collection_index_file(path):
    file_path = os.path.join(path, COLLECTION_MARKER_FILE_NAME)
    if not os.path.exists(file_path):
        write_text_to_file(file_path, "# Dummy file marking this folder as a entity collection folder")


def ensure_folder_exists(folder_path):
    try:
        os.makedirs(folder_path)
    except OSError as ex:
        if ex.errno != errno.EEXIST:
            raise


def split_path(path):
    """splits a path into a list of strings"""
    folders = []
    while 1:
        path, folder = os.path.split(path)
        if folder != "":
            folders.append(folder)
        if path == '':
            break
    folders.reverse()
    return folders


def get_entity_collection_name_and_install_path(obj, attr):
    installation = get_installation(obj, None)
    if installation:
        entity_collection_name = installation.install_path.entity_collection_name
        install_path = installation.install_path.full.replace(':', '-')
    elif attr and has_entity_collection(attr):
        entity_collection_name = get_entity_collection(attr)
        install_path = ''
    else:
        # todo: fix me
        entity_collection_name = "frames"  # hack to put global items in frames, until we get a global area in the docs
        install_path = ''
        # raise RuntimeError("Unable to determine the entity collection for method %s" % attr)
    return entity_collection_name, install_path


def get_rst_folder_path(rst_root_path, obj, attr=None):
    entity_collection_name, install_path = get_entity_collection_name_and_install_path(obj, attr)
    collection_path = os.path.join(rst_root_path, entity_collection_name)
    if not os.path.exists(collection_path):
        raise RuntimeError("Documentation collections folder %s does not exist" % collection_path)
    write_dummy_collection_index_file(collection_path)
    install_path_parts = split_path(install_path)
    path = collection_path
    for part in install_path_parts:
        path = os.path.join(path, part)
        ensure_folder_exists(path)
    return path


#############################################################################
# python rst

def get_index_ref(cls):
    installation = get_installation(cls, None)
    nested_level = len(installation.install_path._intermediate_names) if installation else 0
    return ("../" * nested_level) + 'index'


def get_rst_attr_file_header(class_name, index_ref, attr_name):
    """produce the rst content to begin an attribute-level *.rst file"""
    # use  :doc:`class_name<index>` syntax to create reference back to the index.rst file
    display_attr_name = '__init__' if is_entity_constructor_command_name(attr_name) else attr_name
    title = ":doc:`%s <%s>`  %s" % (class_name, index_ref, display_attr_name)
    return get_rst_file_header(title)


def get_rst_file_header(title):
    title_emphasis = '*' * len(title)
    return """
{title}
{title_emphasis}

------

""".format(title=title, title_emphasis=title_emphasis)


def get_rst_cls_file_header(collection_name, class_name):
    """produce the rst content to begin an attribute-level *.rst file"""
    # use  :doc:`class_name<index>` syntax to create reference back to the index.rst file
    title = ":doc:`%s<../index>` %s" % (collection_name.capitalize(), class_name)
    return get_rst_file_header(title)


def write_py_rst_attr_file(root_path, cls, attr):
    command_def = attr.command
    header = get_rst_attr_file_header(class_name=get_type_name(cls), index_ref=get_index_ref(cls), attr_name=attr.__name__) #command_def.name)
    content = get_command_def_rst(command_def)
    folder = get_rst_folder_path(root_path, cls, attr)
    file_path = os.path.join(folder, command_def.name + ".rst")
    write_text_to_file(file_path, [header, content])


def write_py_rst_cls_file(root_path, cls):
    installation = get_installation(cls)
    entity_collection_name = installation.install_path.entity_collection_name
    header = get_rst_cls_file_header(entity_collection_name, class_name=get_type_name(cls))
    content = get_class_rst(cls)
    folder = get_rst_folder_path(root_path, cls)
    file_path = os.path.join(folder, "index.rst")
    write_text_to_file(file_path, [header, content])


def write_py_rst_collections_file(root_path, collection_name, subfolders, files):
    # go through subfolders and find the index.rst and add to toc jazz
    # go through files and list global methods  (leave making nice "summary table" as another exercise)
    title = upper_first(collection_name)
    title_emphasis = "=" * len(title)
    # Frame <frame-/index.rst>
    toctree = indent("\n".join(["%s <%s/index.rst>" % (entity_type_to_class_name(subfolder.replace('-',':')), subfolder) for subfolder in subfolders]))
    names =[f[:-4] for f in files if f[-4:] == ".rst" and f != "index.rst"]
    globs = "\n\n".join([":doc:`%s<%s>`" % (name, name) for name in names])
    hidden_toctree = indent("\n".join(names))
    content = """
{title}
{title_emphasis}

**Classes**

.. toctree::
{toctree}

.. toctree::
    :hidden:

{hidden_toctree}

-------

**Global Methods**

{globs}
""".format(title=title, title_emphasis=title_emphasis, toctree=toctree, hidden_toctree=hidden_toctree, globs=globs)
    file_path = os.path.join(root_path, "index.rst")
    write_text_to_file(file_path, content)


def walk_py_rst_collection_folders(root_path):
    for folder, subfolders, files in os.walk(root_path):
        if COLLECTION_MARKER_FILE_NAME in files:
            write_py_rst_collections_file(folder, os.path.split(folder)[1], subfolders, files)


def get_attr_py_rst(doc_root_path):
    root_path = doc_root_path

    def write_command(obj, attr):
        if hasattr(attr, "command"):
            write_py_rst_attr_file(root_path, obj, attr)
    return write_command


def get_obj_py_rst(doc_root_path):
    root_path = doc_root_path

    def write_class(cls):
        write_py_rst_cls_file(root_path,  cls)
    return write_class


##############################################################################
# REST API

def write_command_def_rest_rst_file(root_path, command_def):
    content = get_command_def_rest_rst(command_def)
    file_path = os.path.join(root_path, get_command_rest_rst_file_name(command_def))
    write_text_to_file(file_path, content)


def write_commands_index_rest_rst_file(root_path, command_defs):
    content = get_commands_rest_index_content(command_defs)
    file_path = os.path.join(root_path, "index.rst")
    write_text_to_file(file_path, content)


##############################################################################
# SPA

doc_stub_class_to_members_text_dict = {}   # obj -> list of docstub text per method, ex.
#  { Frame -> [ "@docstub\ndef add_columns....", "@docstub\ndef ecdf(..." ...) ],
#    VertexFrame -> [ "...  " ...], ... }


doc_stub_import_return_types = set()  # for spa to work (at least in IJ), the rtype must be in the modules namespace,
# for example, get_frame returns a Frame.  For IJ to provide metadata about the return type, Frame has to be around
# so we hold on to all the funky return types so we can import them in the docstubs.


def attr_spa(obj, attr):
    if hasattr(attr, "command"):
        if obj not in doc_stub_class_to_members_text_dict:
            obj_spa(obj)
        command_def = attr.command
        if command_def.is_constructor:
            text = get_doc_stub_init_text(command_def, override_rtype=obj)
        else:
            text = get_function_text(command_def, decorator_text='@doc_stub')
        if command_def.return_info:
            return_type = command_def.return_info.data_type
            if return_type:
                s = return_type.__name__ if hasattr(return_type, "__name__") else str(return_type) if not isinstance(return_type, basestring) else return_type
                if s not in dir(__builtins__) and s not in doc_stub_import_return_types:
                    doc_stub_import_return_types.add(return_type)
        doc_stub_class_to_members_text_dict[obj].append((command_def.name, text))


def obj_spa(obj):
    if obj not in doc_stub_class_to_members_text_dict:
        doc_stub_class_to_members_text_dict[obj] = []


###################################################################

# python
delete_folder(dst_python_api_dir)
copy_template(python_api)
path = dst_python_api_dir
print "Creating rst files for Python API docs, using root_path %s" % path
ta._walk_api(get_obj_py_rst(path), get_attr_py_rst(path), include_init=True)
walk_py_rst_collection_folders(path)  # this patches things up, writes some index.rst files

# rest
delete_folder(dst_rest_api_dir)
copy_template(rest_api)
path = dst_rest_api_commands_dir  # the autogen stuff is just for commands/
print "Creating rst files for REST API docs, using root_path %s" % path
server_version, server_commands = download_server_details(ta.server)  # rather than walk api, use all the defs from the server
for c in server_commands:
    write_command_def_rest_rst_file(path, c)
write_commands_index_rest_rst_file(path, server_commands)

# spa
print "Creating spa modules..."
ta._walk_api(obj_spa, attr_spa, include_init=True)
text_1, text_2 = get_doc_stub_modules_text(doc_stub_class_to_members_text_dict, doc_stub_import_return_types)
write_text_to_file(spa_module1_file_name, text_1)
write_text_to_file(spa_module2_file_name, text_2)
print "Done."
