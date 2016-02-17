# vim: set encoding=utf-8

#
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
gendoct.py

Generates a file of testcases which run the Python API documentation examples through doctest

https://docs.python.org/2/library/doctest.html

This is not a test content file itself, but a test generator as well as a library, which is why
it sits in the same folder as the test files.

There are 2 source areas for the documentation examples:

1. The .rst files found recursively in doc-api-examples/src/main/resources/python

2. The specific API .py files in python-client/trustedanalytics/core

This module locates all the files for doctest testing.  It module writes a new file
containing code text for unittest testcase classes and testcase methods for
each of the files it found.  So, multiple functions are distributed among several
classes.  This helps nose runner concurrency performance.

The test case method simply calls back to this module with its path.  So it is this
module which actually loads the file content, runs a preprocessor on it which edits the
text appropriately for running in doctest (this enables skipping some test content or adding
ELLIPSIS_MARKERs to ignore some output), and then calls doctest.

Generating a test file was chosen because of difficulties working with nose in parallel
execution for dynamically generated code.

(see integration-tests/README_doctest.md)
"""

# Debug options
doctest_verbose = False     # set to True for debug if you want to see all the comparisons
doctest_print_time = False   # set to True to have the execution time printed for each doctest file


import time
import fnmatch

import trustedanalytics as ta
if ta.server.port != 19099:
    ta.server.port = 19099

from trustedanalytics.meta.doc import parse_for_doctest, DocExamplesPreprocessor

import doctest
doctest.ELLIPSIS_MARKER = DocExamplesPreprocessor.doctest_ellipsis


# file path calculations
import os
this_script_name = os.path.basename(__file__)
this_script_as_module_name = os.path.splitext(__file__)[0]
here = os.path.dirname(os.path.abspath(__file__))
path_to_atk_root = os.path.dirname(os.path.dirname(here))
path_to_examples = os.path.join(path_to_atk_root, "doc-api-examples/src/main/resources/python")
path_to_core = os.path.join(path_to_atk_root, "python-client/trustedanalytics/core")
trim_to_atk_root_len = len(path_to_atk_root) + 1   # +1 for slash

def _trim_test_path(path):
    return path[trim_to_atk_root_len:]

test_file_name = os.path.join(here, "doc_api_examples_test_generated.py")  # the name of generated test module
# note: the *_generated.py* is a pattern in the root .gitignore file as well as the pom for maven-clean-plugin



# file exemptions - relative paths of those .rst files which should be skipped
# todo: repair the following .rst files to run correctly as doctests as possible

exemptions = set("""
model/rename.rst
frame-vertex/drop_duplicates.rst
frame-/rename_columns.rst
frame-/label_propagation.rst
graph/graphx_connected_components.rst
graph/graphx_triangle_count.rst
graph/graphx_pagerank.rst
frame/column_median.rst
frame/column_mode.rst
frame/column_summary_statistics.rst
frame/drop_columns.rst
frame/export_to_csv.rst
frame/export_to_hbase.rst
frame/export_to_hive.rst
frame/export_to_jdbc.rst
frame/export_to_json.rst
frame/rename.rst
model-daal_k_means/new.rst
model-daal_k_means/train.rst
model-daal_k_means/test.rst
model-daal_k_means/publish.rst
""".splitlines())


def filter_exemptions(paths):
    """returns the given paths with the exemptions removed"""
    chop = len(path_to_examples) + 1  # the + 1 is for the extra forward slash
    filtered_paths = [p for p in paths if p[chop:] not in exemptions]
    return filtered_paths


def get_all_example_rst_file_paths():
    """walks the path_to_examples and creates paths to all the .rst files found"""
    paths = []
    for root, dir_names, file_names in os.walk(path_to_examples):
        for filename in fnmatch.filter(file_names, '*.rst'):
            path = os.path.join(root, filename)
            if not path.startswith(path_to_atk_root):
                raise RuntimeError("doctest target '%s' does not have expected prefix '%s'" % (path, path_to_atk_root))
            paths.append(path)
    return paths


def _get_cleansed_test_text(full_path):
    """parses the file at the given path and returns a cleansed string of its test content"""
    with open(full_path) as test_file:
        content = test_file.read()
    cleansed = parse_for_doctest(content)
    return cleansed


# ------------------------------------------------------------------------------------------
# This next function was taken straight from doctest source and modified to return results
def _run_docstring_examples(f, globs, verbose=False, name="NoName", compileflags=None, optionflags=0):
    """
    Test examples in the given object's docstring (`f`), using `globs`
    as globals.  Optional argument `name` is used in failure messages.
    If the optional argument `verbose` is true, then generate output
    even if there are no failures.

    `compileflags` gives the set of flags that should be used by the
    Python compiler when running the examples.  If not specified, then
    it will default to the set of future-import flags that apply to
    `globs`.

    Optional keyword arg `optionflags` specifies options for the
    testing and output.  See the documentation for `testmod` for more
    information.
    """
    # Find, parse, and run all tests in the given module.
    finder = doctest.DocTestFinder(verbose=verbose, recurse=False)
    runner = doctest.DocTestRunner(verbose=verbose, optionflags=optionflags)
    for test in finder.find(f, name, globs=globs):
        runner.run(test, compileflags=compileflags)

    # ATK adds these two lines:
    runner.summarize()
    return doctest.TestResults(runner.failures, runner.tries)
# ------------------------------------------------------------------------------------------


def run(test_path):
    """
    Executes doctest on the given file path, which is relative to ATK root path, returns results

    Is imported and called by generated code
    """
    path = os.path.join(path_to_atk_root, test_path)
    text = _get_cleansed_test_text(path)
    start = time.time()
    results = _run_docstring_examples(text,
                                      globs={},
                                      verbose=doctest_verbose,
                                      name=path,
                                      optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    if doctest_print_time:
        print "%2.3f seconds for doctest on %s\n" % (time.time() - start, path)

    return results


file_header = '''
"""
Auto-generated by %s.  *EDITS WILL BE LOST*

Contains test cases for doctest execution
"""

import unittest

from %s import %s

''' % (this_script_name, this_script_as_module_name, run.__name__)


def get_testcase_class_text(suffix):
    return '''
class ExampleDocTests%s(unittest.TestCase):
    """Container class to hold the test cases as functions added by meta-programming"""
''' % suffix

file_footer = '''

if __name__ == "__main__":
    unittest.main()
'''


def replace_invalid_chars(s):
    """replaces chars unsuitable for a python name with '_' """
    return ''.join([c if c.isalnum() or c == '_' else '_' for c in s])


def _create_test_text(full_test_path):
    """
    returns string of python code which should execute a doctest for given test_path

    :param full_test_path: string indicating the full path location of the test source file
    """
    path_from_root = _trim_test_path(full_test_path)
    test_name = 'test_' + replace_invalid_chars(path_from_root)
    return """
    def %s(self):
        test_path = '%s'
        results = %s(test_path)
        self.assertEqual(0, results.failed, test_path)
""" % (test_name, path_from_root, run.__name__)


def main():
    """Write a new file containing doctest test cases"""

    try:
        pyc = test_file_name + 'c'
        os.remove(pyc)  # remove *.pyc
    except:
        pass
    else:
        print "[%s] Removed pre-existing .pyc file %s" % (this_script_name, pyc)

    rst_paths = get_all_example_rst_file_paths()
    filtered_rst_paths = filter_exemptions(rst_paths)
    core_py_paths = [os.path.join(path_to_core, "frame.py"),
                     os.path.join(path_to_core, "graph.py")]  # note: nothing really interesting in model.py itself

    tests = [_create_test_text(p) for p in (filtered_rst_paths + core_py_paths)]

    class_count = 0
    functions_per_class = 10

    with open(test_file_name, 'w') as test_file:
        test_file.write(file_header)
        for i in xrange(0, len(tests), functions_per_class):
            test_file.write(get_testcase_class_text(suffix=str(class_count)))
            test_file.writelines(tests[i:i+functions_per_class])
            class_count += 1
        test_file.write(file_footer)

    print "[%s] Wrote test cases for %d files to %s." % (this_script_name, len(tests), test_file_name)


if __name__ == "__main__":
    main()
