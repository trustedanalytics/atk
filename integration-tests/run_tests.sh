#!/bin/bash
#
# This script executes all of the tests located in this folder through the use
# of the nosetests api.
#
# Use '-c' option to run with coverage

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
PYTHON_DIR='/usr/lib/python2.7/site-packages'
TARGET_DIR=$DIR/target
OUTPUT1=$TARGET_DIR/surefire-reports/TEST-nose-smoketests.xml
OUTPUT2=$TARGET_DIR/surefire-reports/TEST-nose-tests.xml
OUTPUT3=$TARGET_DIR/surefire-reports/TEST-nose-nonconcurrenttests.xml
export PYTHONPATH=$DIR/../python-client:$PYTHONPATH:$PYTHON_DIR

echo "$NAME DIR=$DIR"
echo "$NAME PYTHON_DIR=$PYTHON_DIR"
echo "$NAME PYTHONPATH=$PYTHONPATH"
echo "$NAME all generated files will go to target dir: $TARGET_DIR"

echo "$NAME Shutting down old API Server if it is still running"
$DIR/rest-server-stop.sh


if [ ! -d $TARGET_DIR/surefire-reports/ ]
then
    echo "$NAME Creating target dir"
    mkdir -p $TARGET_DIR/surefire-reports/
fi


# define the nosetests commands here:
NOSE_SMOKE="nosetests $DIR/smoketests --nologcapture --with-xunitmp --xunitmp-file=$OUTPUT1 --processes=10 --process-timeout=300 --with-isolation"
NOSE="nosetests $DIR/tests --nologcapture --with-xunitmp --xunitmp-file=$OUTPUT2 --processes=10 --process-timeout=300 --with-isolation"
NOSE_NONCONCURRENT="nosetests $DIR/nonconcurrenttests --nologcapture --with-xunitmp --xunitmp-file=$OUTPUT3 --process-timeout=300 --with-isolation"

if [ "$1" = "-c" -o "$2" = "-c"] ; then
    echo "$NAME Running with coverage ENABLED.  (runs in a single process, takes a bit longer)"
    COVERAGE_ARGS_BASE="--with-coverage --cover-package=trustedanalytics --cover-erase --cover-inclusive --cover-html"
    COVERAGE_ARGS_SMOKE="$COVERAGE_ARGS_BASE --cover-html-dir=$TARGET_DIR/surefire-reports/cover-smoke"
    COVERAGE_ARGS="$COVERAGE_ARGS_BASE --cover-html-dir=$TARGET_DIR/surefire-reports/cover"
    NOSE_SMOKE="nosetests $DIR/smoketests $COVERAGE_ARGS_SMOKE --nologcapture --with-xunitmp --xunitmp-file=$OUTPUT1"
    NOSE="nosetests $DIR/tests $COVERAGE_ARGS --nologcapture --with-xunitmp --xunitmp-file=$OUTPUT2"
fi

if [ "$1" = "-skipnc" -o "$2" = "-skipnc" ] ; then
    SKIP_NC=1
else
    SKIP_NC=0
fi

echo "$NAME remove old trustedanalytics"
rm -rf $TARGET_DIR/fs-root/trustedanalytics

echo "$NAME Starting REST server... "
$DIR/rest-server-start.sh
STARTED=$?
if [[ $STARTED == 2 ]]
then
    echo "$NAME FAILED Couldn't start REST server"
    exit 2
fi

sleep 10
PORT=19099
COUNTER=0
httpCode=$(curl -s -o /dev/null -w "%{http_code}" localhost:$PORT)
echo "$NAME REST Server http status code was $httpCode"
while [ $httpCode != "200" ]
do
    if [ $COUNTER -gt 120 ]
    then
        echo "$NAME Tired of waiting for REST Server to start up, giving up..."
        $DIR/rest-server-stop.sh
        exit 3
    else
        let COUNTER=COUNTER+1
    fi
    echo "$NAME Waiting for REST Server to start up on port $PORT..."
    sleep 1
    httpCode=$(curl -s -o /dev/null -w "%{http_code}" localhost:$PORT)
    echo "$NAME REST Server http status code was $httpCode"
done

echo "$NAME nosetests will be run in three calls: 1) make sure system works in basic way, 2) the tests that can run concurrently, 3) and the nonconcurrent tests collection tests (use -skipnc to skip the nonconcurrent tests, like garbage collection)."
sleep 10
# Rene said each build agent has about 18 cores (Feb 2015)

echo "$NAME 1) Running smoke tests to verify basic functionality needed by all tests, calling nosetests"
echo "$NAME $NOSE_SMOKE"
eval $NOSE_SMOKE
SMOKE_TEST_SUCCESS=$?

if [[ $SMOKE_TEST_SUCCESS == 0 ]] ; then
    echo "$NAME Python smoke tests PASSED -- basic frame,graph,model functionality verified"
    echo "$NAME 2) Running the main suite of tests, calling nosetests again"
    echo "$NAME $NOSE"
    eval $NOSE
    TEST_SUCCESS=$?
fi
if [ $TEST_SUCCESS = 0 -a $SKIP_NC = 0 ] ; then 
    echo "$NAME 3) The main suite of tests PASSED, calling nosetests again for the nonconcurrent tests"
    echo "$NAME $NOSE_NONCONCURRENT"
    eval $NOSE_NONCONCURRENT
    NC_TEST_SUCCESS=$?
else
    NC_TEST_SUCCESS=0
fi

$DIR/rest-server-stop.sh

if [[ $SMOKE_TEST_SUCCESS != 0 ]] ; then
   echo "$NAME Python smoke tests FAILED"
   echo "$NAME bailing out early, no reason to run any other tests if smoke tests are failing"
   echo "$NAME see nosetest output: $OUTPUT1"
   echo "$NAME also see log output in target dir"
   exit 1
elif [[ $TEST_SUCCESS != 0 ]] ; then
   echo "$NAME Python feature tests FAILED"
   echo "$NAME see nosetest output: $OUTPUT2 "
   echo "$NAME also see log output in target dir"
   exit 2
elif [[ $NC_TEST_SUCCESS != 0 ]] ; then
   echo "$NAME Python nonconcurrent tests FAILED"
   echo "$NAME see nosetest output: $OUTPUT3 "
   echo "$NAME also see log output in target dir"
   exit 3
else
   echo "$NAME All Python tests PASSED"
   exit 0
fi
