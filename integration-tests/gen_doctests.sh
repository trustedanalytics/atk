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
# Runs the python script to generate the doctest file
#

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "$NAME DIR=$DIR"
export PYTHONPATH=$DIR/../python-client:$PYTHONPATH:$PYTHON_DIR


pushd $DIR/tests
python2.7 gendoct.py

SUCCESS=$?
if [[ $SUCCESS != 0 ]]
then
    echo "gendoct.py failed"
    exit 1
fi

popd

