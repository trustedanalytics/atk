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

# vim: set encoding=utf-8

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
runs one or more doc-api_examples .rst files as doctest
"""

usage = """
USAGE: python run_doctest.py  <command .rst>

The .rst path relative to doc-api-examples/src/main/resources/python/.
Make sure integration test server is running first.  Example:

    integration-tests$ ./rest-server-start.sh
    integration-tests$ ipython run_doctest.py frame/ecdf.rst

    # to shutdown server
    integration-tests$ ./rest-server-stop.sh

    # can pass multiple files at once
    integration-tests$ ipython run_doctest.py frame/ecdf.rst frame/drop_duplicates.rst
"""


import sys
# doctor the python path...
sys.path.insert(0, "../python-client")
sys.path.insert(0, "./tests")
import os

if len(sys.argv) < 2:
    print usage
    sys.exit(1)

from doc_api_examples_tests import _run_files_as_doctests, path_to_examples

for arg in sys.argv[1:]:
    p = os.path.join(path_to_examples, arg)
    _run_files_as_doctests([p], verbose=True)
