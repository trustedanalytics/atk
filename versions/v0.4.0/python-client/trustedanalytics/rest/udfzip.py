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
This file zips and converts necessary modules to evaluate a lambda expression by looking into UdfDependencies list
"""

import zipfile
import os
import os.path

class UdfZip(object):

    # From http://stackoverflow.com/questions/14438928/python-zip-a-sub-folder-and-not-the-entire-folder-path
    @staticmethod
    def _dirEntries(dir_name, subdir, *args):
        # Creates a list of all files in the folder
        '''Return a list of file names found in directory 'dir_name'
        If 'subdir' is True, recursively access subdirectories under 'dir_name'.
        Additional arguments, if any, are file extensions to match filenames. Matched
            file names are added to the list.
        If there are no additional arguments, all files found in the directory are
            added to the list.
        Example usage: fileList = dirEntries(r'H:\TEMP', False, 'txt', 'py')
            Only files with 'txt' and 'py' extensions will be added to the list.
        Example usage: fileList = dirEntries(r'H:\TEMP', True)
            All files and all the files in subdirectories under H:\TEMP will be added
            to the list. '''

        fileList = []
        for file in os.listdir(dir_name):
            dirfile = os.path.join(dir_name, file)
            if os.path.isfile(dirfile):
                if not args:
                    fileList.append(dirfile)
                else:
                    if os.path.splitext(dirfile)[1][1:] in args:
                        fileList.append(dirfile)
                        # recursively access file names in subdirectories
            elif os.path.isdir(dirfile) and subdir:
                fileList.extend(UdfZip._dirEntries(dirfile, subdir, *args))
        return fileList

    @staticmethod
    def _makeArchive(fileList, archive, root):
        """
        'fileList' is a list of file names - full path each name
        'archive' is the file name for the archive with a full path
        """
        with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for f in fileList:
                zipf.write(f, os.path.relpath(f, root))

    @staticmethod
    def zipdir(path):
        # zips a path to /tmp/iapydependencies.zip. Please note that this zip file will be truncated every time
        # this call is made. So to preserver the contents, read the file immediately or copy. Not thread-safe.
        UdfZip._makeArchive(UdfZip._dirEntries(path, True), '/tmp/iapydependencies.zip', path[0:path.rfind('/')])
