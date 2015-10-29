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
runs the Python API documentation examples through doctest

https://docs.python.org/2/library/doctest.html

There are 2 source areas for the documentation examples:

1. The .rst files found recursively in doc-api-examples/src/main/resources/python

2. The specific API .py files in python-client/trustedanalytics/core

This test module locates all the files for testing and runs a preprocessor on them which edits the
text appropriately for running in doctest.  These edits enable skipping some test content or adding
ELLIPSIS_MARKERs (to ignore some output).

The processed text from each file is loaded into the __test__ variable of the doc_api_examples_dummy_module
and that module is submitted to doctest.

(see integration-tests/README_doctest.md)
"""

import unittest
import time

import trustedanalytics as ta
if ta.server.port != 19099:
    ta.server.port = 19099

from trustedanalytics.meta.doc import parse_for_doctest, DocExamplesPreprocessor
import doc_api_examples_dummy_module as doctests_module

import doctest
doctest.ELLIPSIS_MARKER = DocExamplesPreprocessor.doctest_ellipsis


import os
here = os.path.dirname(os.path.abspath(__file__))
path_to_examples = os.path.join(here, "../../doc-api-examples/src/main/resources/python")
path_to_core = os.path.join(here, "../../python-client/trustedanalytics/core")

import fnmatch


def filter_exemptions(paths):
    """returns the given paths with the exemptions removed"""
    chop = len(path_to_examples) + 1  # the + 1 is for the extra forward slash
    filtered_paths = [p for p in paths if p[chop:] not in doctests_module.exemptions]
    return filtered_paths


def get_all_example_rst_file_paths():
    """walks the path_to_examples and creates paths to all the .rst files found"""
    paths = []
    for root, dirnames, filenames in os.walk(path_to_examples):
        for filename in fnmatch.filter(filenames, '*.rst'):
            paths.append(os.path.join(root, filename))
    return paths


def add_doctest_file(full_path):
    """parses the file at the given path and adds it to the test list for doctest"""
    with open(full_path) as f:
        content = f.read()
    cleansed = parse_for_doctest(content)
    # print "Adding content for %s" % full_path
    doctests_module.__test__[full_path] = cleansed


def _init_doctest_files(files):
    """initializes doctests with the given list of files"""
    if isinstance(files, basestring):
        files = [files]
    doctests_module.__test__.clear()
    for f in files:
        add_doctest_file(f)


def _run_files_as_doctests(files, verbose=False):
    if files:
        print "Running doctest on these files:"
        for f in files:
            print f
    else:
        print "(no Example files found!)"
    _init_doctest_files(files)
    return doctest.testmod(m=doctests_module,
                           raise_on_error=False,
                           exclude_empty=True,
                           verbose=verbose,
                           report=True,
                           optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)


doctest_verbose = False  # set to True for debug if you want to see all the comparisons


class ExampleDocTests(unittest.TestCase):

    # todo: break these up to run in parallel

    def test_doc_examples(self):
        """test all .rst files with doctest"""
        start = time.time()
        files = get_all_example_rst_file_paths()
        filtered_files = []
        filtered_files = filter_exemptions(files)
        filtered_files.extend([os.path.join(path_to_core, "frame.py")])
        filtered_files.extend([os.path.join(path_to_core, "graph.py")])  # todo: add model.py, maybe others
        results = _run_files_as_doctests(filtered_files, verbose=doctest_verbose)
        print
        print "doctest elapsed time: %0.3f seconds." % (time.time() - start)
        self.assertEqual(0, results.failed, "Tests in the example documentation failed.")


if __name__ == "__main__":
    unittest.main()
