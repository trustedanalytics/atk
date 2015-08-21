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

import unittest
import trustedanalytics as atk

# show full stack traces
atk.errors.show_details = True
#atk.loggers.set_api()
# TODO: port setup should move to a super class
if atk.server.port != 19099:
    atk.server.port = 19099
atk.connect()()




#import trustedanalyticsas atka
#atk.server.port = 1909atk.connect()ect()


import doctest
#doctest.ELLIPSIS_MARKER = "..."
doctest.ELLIPSIS_MARKER = "-etc-"

import sys
current_module = sys.modules[__name__]

import os
#print "examples=%s" % examples
here = os.path.dirname(os.path.abspath(__file__))
#path_to_examples = os.path.join(here, "../../python/trustedanalytics/doc/examples")
import fnmatch

__test__ = {}


# option 5
def get_all_example_rst_file_paths():
    paths = []
    for root, dirnames, filenames in os.walk(path_to_examples):
        for filename in fnmatch.filter(filenames, '*.rst'):
            paths.append(os.path.join(root, filename))
    return paths


def clean_for_ignores(line):
    if line.lstrip()[:2] == '[=':
        return "-etc-"
    return line

def add_rst_file(full_path):
    with open(full_path) as f:
        content = f.read()
    cleansed = '\n'.join([clean_for_ignores(line) for line in content.split('\n')])
    print "Adding content for %s" % full_path
    __test__[full_path] = cleansed


def init_tests(files):
    if isinstance(files, basestring):
        files = [files]
    __test__.clear()
    for f in files:
        add_rst_file(f)


def run_tests(files=None, verbose=False):
    init_tests(files or get_all_example_rst_file_paths())
    return doctest.testmod(m=current_module,
                    raise_on_error=False,
                    exclude_empty=True,
                    verbose=verbose,
                    optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)


class ExampleDocTests(unittest.TestCase):

    def test_examples(self):
        print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
        #results = run_tests(["/home/blbarker/dev/atk/doc-api-examples/src/main/resources/python/frame/simple.doctest"], verbose=True)
        results = run_tests(["/home/blbarker/dev/atk/doc-api-examples/src/main/resources/python/frame/ecdf.rst"], verbose=True)
        self.assertEqual(0, results.failed, "Tests in the example documentation failed.")
        print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"


if __name__ == "__main__":
    unittest.main()


#run_tests(verbose=True)

# option 4
# def run_example(relative_path, verbose=False):
#     here = os.path.dirname(os.path.abspath(__file__))
#     path_to_examples = os.path.join(here, "../../python/trustedanalytics/doc/examples")
#
#     __test__['bin_column'] = content
#     print "Running examples in %s" % relative_path
#     return doctest.testmod(exclude_empty=True,
#                            verbose=verbose,
#                            optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
#
# bin_column_path = "frame/bin_column.rst"
#
# result = run_example(bin_column_path)
#
# print "result=%s" % str(result)
# print "failed=%s" % result.failed


# option 3
# def run_example(relative_path):
#     with open(os.path.join(path_to_examples, "frame/bin_column.rst")) as f:
#         content = f.read()
#
#     print "Running examples in %s" % relative_path
#     return doctest.run_docstring_examples(content,
#                                           {"ta": ta},
#                                           verbose=True,
#                                           optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
#
# bin_column_path = "frame/bin_column.rst"
#
# result = run_example(bin_column_path)
#
# print "result=%s" % result


# option 2
#connection_header = """
#>>> import trustedanalyticas atkta
#>>> atk.server.port = 19099atk.connect()onnect()
#-etc-
#"""
#content = "\n".join([connection_header, content])
#doctest.run_docstring_examples(content, {}, verbose=True, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)

# option 1
#doctest.testfile(examples + "/frame/bin_column.rst", module_relative=False, verbose=True, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
