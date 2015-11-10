#!/bin/bash
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


# Executes all of the tests defined in this doc folder, using Python's doctest

if [[ -f /usr/lib/TrustedAnalytics/virtpy/bin/activate ]]; then
    ACTIVATE_FILE=/usr/lib/TrustedAnalytics/virtpy/bin/activate
else
    ACTIVATE_FILE=/usr/local/virtpy/bin/activate
fi

if [[ ! -f $ACTIVATE_FILE ]]; then
    echo "Virtual Environment is not installed please execute install_pyenv.sh to install."
    exit 1
fi

source $ACTIVATE_FILE

TESTS_DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"
ATK=`dirname $TESTS_DIR`
DOC=$IA/doc
SOURCE_CODE_PYTHON=`dirname $IA`

echo SOURCE_CODE_PYTHON=$SOURCE_CODE_PYTHON

cd $SOURCE_CODE_PYTHON 

python -m doctest -v $DOC/source/examples.rst

success=$?

rm *.log 2> /dev/null

if [[ $success == 0 ]] ; then
   echo "Python Doc Tests Successful"
   exit 0
fi
echo "Python Doc Tests Unsuccessful"
exit 1

