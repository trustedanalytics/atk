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
Initialization for any unit test
"""
import os
import sys
import logging


class TestFolders(object):
    """Folder paths for the tests"""
    def __init__(self):
        dirname = os.path.dirname
        self.here = dirname(__file__)
        self.tmp = os.path.join(self.here, "tmp")
        self.conf = os.path.join(self.here, "conf")
        self.root = dirname(dirname(self.here))  # parent of trusted_analytics

    def __repr__(self):
        return '{' + ",".join(['"%s": "%s"' % (k, v)
                               for k, v in self.__dict__.items()]) + '}'


folders = TestFolders()


def init():
    if sys.path[1] != folders.root:
        sys.path.insert(1, folders.root)


def set_logging(logger_name, level=logging.DEBUG):
    """Sets up logging for the test"""
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    h = logging.StreamHandler()
    h.setLevel(logging.DEBUG)
    logger.addHandler(h)
