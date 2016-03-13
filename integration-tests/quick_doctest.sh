#!/bin/bash
#
#  Copyright (c) 2016 Intel Corporation 
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

#
# This script calls embedded python to execute doctest on a single file
#

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "$NAME invoking python..."

python2.7 - $1 $2 <<END
usage = """
USAGE: quick_doctest.sh [-atk] path

The path is relative to doc-api-examples/src/main/resources/python/.

If the '-atk' flag is used, then the path is relative to the root atk folder

Make sure the integration-tests rest server is running.

Example:

    $ ./rest-server-start.sh
    $ quick_doctest.sh frame/ecdf.rst
    $ quick_doctest.sh -atk python-client/trustedanalytics/core/frame.py

"""

import sys
if len(sys.argv) < 2:
    print usage
    sys.exit(1)

is_example_path = '-atk' not in sys.argv[1:-1]
file_path = sys.argv[-1]

# doctor the python path
sys.path.insert(0, "../python-client")
sys.path.insert(0, "./tests")

import os
import gendoct  # gendoct import also sets the port for integration tests
import trustedanalytics as ta
# print "port=" + str(ta.server.port)
ta.connect()

gendoct.doctest_verbose = True

if is_example_path:
    file_path = os.path.join(gendoct.path_to_examples, file_path)

results = gendoct.run(file_path)
print repr(results)
END

SUCCESS=$?
if [[ $SUCCESS != 0 ]]
then
    echo "$NAME Failed"
    exit 1
fi

