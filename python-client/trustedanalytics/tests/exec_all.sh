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


# This script executes all of the tests located in this folder through the use
# of the nosetests api. Coverage is provided by coverage.py
# This script requires the installation of the install_pyenv.sh script

# '-x' to exclude dirs from coverage, requires nose-exclude to be installed

export IN_UNIT_TESTS='true'

TESTS_DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"
TRUSTEDANALYTICS_DIR=`dirname $TESTS_DIR`
PYTHON_DIR=`dirname $TRUSTEDANALYTICS_DIR`

echo TESTS_DIR=$TESTS_DIR
echo TRUSTEDANALYTICS_DIR=$TRUSTEDANALYTICS_DIR
echo PYTHON_DIR=$PYTHON_DIR

cd $PYTHON_DIR
export PYTHONPATH=$PYTHONPATH:$PYTHON_DIR
#python -c "import sys; print 'sys.path=' +  str(sys.path)"

# check if the python libraries are correctly installed by importing
# them through python. If there is no output then the module exists.
if [[ -e $(python2.7 -c "import trustedanalytics") ]]; then
    echo "trustedanalytics cannot be found"
    exit 1
fi

if [[ -e $(python2.7 -c "import nose") ]]; then
    echo "Nosetests is not installed into your python virtual environment please install nose."
    exit 1
fi

if [[ -e $(python2.7 -c "import coverage") ]]; then
    echo "Coverage.py is not installed into your python virtual environment please install coverage."
    exit 1
fi


rm -rf $PYTHON_DIR/cover

if [ "$1" = "-x" ] ; then
  EXCLUDE_DIRS_FILE=$TESTS_DIR/cov_exclude_dirs.txt
  if [[ ! -f $EXCLUDE_DIRS_FILE ]]; then
    echo ERROR: -x option: could not find exclusion file $EXCLUDE_DIRS_FILE
    exit 1
  fi
  echo -x option: excluding files from coverage described in $EXCLUDE_DIRS_FILE
  EXCLUDE_OPTION=--exclude-dir-file=$EXCLUDE_DIRS_FILE
fi


nosetests $TESTS_DIR --with-coverage --cover-package=trustedanalytics --cover-erase --cover-inclusive --cover-html --with-xunit  --xunit-file=$PYTHON_DIR/nosetests.xml $EXCLUDE_OPTION

success=$?

COVERAGE_ARCHIVE=$PYTHON_DIR/python-coverage.zip

rm *.log 2> /dev/null
rm -rf $COVERAGE_ARCHIVE 
zip -rq $COVERAGE_ARCHIVE .

RESULT_FILE=$PYTHON_DIR/nosetests.xml
COVERAGE_HTML=$PYTHON_DIR/cover/index.html

echo 
echo Output File: $RESULT_FILE
echo Coverage Archive: $COVERAGE_ARCHIVE
echo Coverage HTML: file://$COVERAGE_HTML
echo 

unset IN_UNIT_TESTS

if [[ $success == 0 ]] ; then
   echo "Python Tests Successful"
   exit 0
fi
echo "Python Tests Unsuccessful"
exit 1

