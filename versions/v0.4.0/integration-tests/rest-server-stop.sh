#!/bin/bash
#
# Shut down rest server that might be running in the background
#

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
TARGET_DIR=$DIR/target
PID_FILE=$TARGET_DIR/rest-server.pid

if [ -e $TARGET_DIR/rest-server.pid ]
then
    API_SERVER_PID=`cat $PID_FILE`
    echo
    echo "$NAME Shutting down API Server, PID was $API_SERVER_PID"
    kill $API_SERVER_PID

    COUNTER=0
    while ps -p $API_SERVER_PID > /dev/null
    do
        sleep 1
        echo "$NAME API Sever is still running..."
        if [ $COUNTER -gt 20 ]
        then
            echo "$NAME Sending kill -9 to API Sever"
            kill -9 $API_SERVER_PID
            exit 1
        else
            let COUNTER=COUNTER+1
        fi
    done

    echo "$NAME API Server should be shutdown now if it wasn't already"
else
    echo "$NAME no pid file found $PID_FILE"
fi
