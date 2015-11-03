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

#
# This script executes one test through nosetests api and proper Python paths --use for debug.
#
NUM_PROCESSES=2

NAME="[`basename $0`]"

if [ "$#" -ne 1 ]
then
  echo "Usage: $NAME filename.py:testsuite.testname"
  exit 1
fi

TEST="$1"
DIR="$( cd "$( dirname "$0" )" && pwd )"
PYTHON_DIR='/usr/lib/python2.7/site-packages'
TARGET_DIR=$DIR/target
OUTPUT=$TARGET_DIR/debug_one_test.xml
export PYTHONPATH=$DIR/../python:$PYTHONPATH:$PYTHON_DIR

echo "$NAME TEST=$TEST"
echo "$NAME DIR=$DIR"
echo "$NAME PYTHON_DIR=$PYTHON_DIR"
echo "$NAME PYTHONPATH=$PYTHONPATH"
echo "$NAME output going to: $OUTPUT"

echo "$NAME Running test $TEST"
nosetests $TEST --nocapture --nologcapture --with-xunitmp --xunitmp-file=$OUTPUT --processes=$NUM_PROCESSES --process-timeout=90 --with-isolation
TEST_SUCCESS=$?

if [[ $TEST_SUCCESS == 0 ]] ; then
   echo "$NAME Python test $TEST PASSED"
   exit 0
else
   echo "$NAME Python test $TEST FAILED"
   exit 1
fi
