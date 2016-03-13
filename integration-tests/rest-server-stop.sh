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
        echo "$NAME API Server is still running..."
        if [ $COUNTER -gt 20 ]
        then
            echo "$NAME Sending kill -9 to API Server"
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
