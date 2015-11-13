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

#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

LAUNCHER=$DIR/../conf/:$DIR/../misc/launcher/target/launcher.jar:.

pushd $DIR/..
pwd

export HOSTNAME=`hostname`
export YARN_CONF_DIR="/etc/hadoop/conf"

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

echo java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot rest-server
java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot rest-server

popd
