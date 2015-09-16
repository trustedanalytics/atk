#!/bin/bash

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

