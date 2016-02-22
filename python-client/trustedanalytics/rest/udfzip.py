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
This file zips and converts necessary python dependencies modules for evaluating Python UDFs,
which dependencies are defined in trustedanalytics.udf_dependencies, which is a list, wrapped
by the UdfDependencies class below, just to provide a docstring.
"""

import zipfile
import uuid
import os
import base64
import os.path
from types import ModuleType


class UdfDependencies(list):
    """
    list of files and/or folders identifying the python dependencies that get
    sent to the cluster

    The items that can be sent to the cluster can be either local python
    scripts without any packaging structure, or folders containing modules
    or packages.

    Examples:

        ``udf_dependencies.append('my_script.py')`` - where my_script.py is
            in the same local folder.  Only the script will be serialized.

        ``udf_dependencies.append('testcases/auto_tests/my_script.py')``  -
            path to a valid python package/module which includes the intended
            python file and all its dependencies.  There are certain pitfalls
            associated with this approach: In this case all the folders,
            subfolders and files within 'testcases' directory get zipped,
            serialized and copied over to each of the worker nodes every time
            that the user calls a function that uses a UDF.

    To keep the overhead low, it is advised that the users make a separate
    folder to hold their python libraries and just add that folder to the
    udf_depenedencies.

    Also, when you do not need the dependencies for future function calls,
    remove them from the list.

    Note: this approach does not work for imports that use relative paths.
    """
    pass  # note this list-wrapping class is used solely for the purpose of providing runtime documentation


# From http://stackoverflow.com/questions/14438928/python-zip-a-sub-folder-and-not-the-entire-folder-path
def _get_dir_entries(dir_name, subdir, *args):
    """
    Return a list of file names found in directory 'dir_name'
    If 'subdir' is True, recursively access subdirectories under 'dir_name'.
    Additional arguments, if any, are file extensions to match filenames. Matched
        file names are added to the list.
    If there are no additional arguments, all files found in the directory are
        added to the list.
    Example usage: fileList = dirEntries(r'H:\TEMP', False, 'txt', 'py')
        Only files with 'txt' and 'py' extensions will be added to the list.
    Example usage: fileList = dirEntries(r'H:\TEMP', True)
        All files and all the files in subdirectories under H:\TEMP will be added
        to the list.
    """
    file_list = []
    for file in os.listdir(dir_name):
        dirfile = os.path.join(dir_name, file)
        if os.path.isfile(dirfile):
            if not args:
                file_list.append(dirfile)
            else:
                if os.path.splitext(dirfile)[1][1:] in args:
                    file_list.append(dirfile)
                    # recursively access file names in subdirectories
        elif os.path.isdir(dirfile) and subdir:
            file_list.extend(_get_dir_entries(dirfile, subdir, *args))
    return file_list


def _make_archive(file_list, archive, root):
    """
    'file_list' is a list of file names - full path each name
    'archive' is the file name for the archive with a full path
    """
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for f in file_list:
            zipf.write(f, os.path.relpath(f, root))


def _zip_dir(path):
    """
    zips a path to /tmp/iapydependencies-<UUID>.zip and returns that path.
    Name is hardened to allow for concurrency.
    """
    file_path = '/tmp/iapydependencies-' + uuid.uuid1().hex + ".zip"
    _make_archive(_get_dir_entries(path, True), file_path, path[0:path.rfind('/')])
    return file_path


def _get_file_content_as_str(filename):

    if isinstance(filename, ModuleType) and hasattr(filename, '__path__'):  # Serialize modules
        file_path = _zip_dir(filename.__path__)
        name, file_to_serialize = ('%s.zip' % os.path.basename(filename), file_path)
    elif isinstance(filename, ModuleType) and hasattr(filename, '__file__'): # Serialize single file based modules
        name, file_to_serialize = (filename.__file__, filename.__file__)
    elif os.path.isdir(filename): # Serialize local directories
        file_path = _zip_dir(filename)
        name, file_to_serialize = ('%s.zip' % os.path.basename(filename), file_path)
    elif os.path.isfile(filename) and filename.endswith('.py'): # Serialize local files
        name, file_to_serialize = (filename, filename)
    else:
        raise Exception('%s should be either local python script without any packaging structure \
        or the absolute path to a valid python package/module which includes the intended python file to be included and all \
                        its dependencies.' % filename)
    # Serialize the file contents and send back along with the new serialized file names
    with open(file_to_serialize, 'rb') as f:
        return (name, base64.urlsafe_b64encode(f.read()))


def get_dependencies_content():
    """returns a list of maps of file_name to file_content"""
    import trustedanalytics
    dependencies = []
    for filename in trustedanalytics.udf_dependencies:
        name, content = _get_file_content_as_str(filename)
        dependencies.append({'file_name': name, 'file_content': content})
    return dependencies
