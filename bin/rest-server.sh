#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

LAUNCHER=$DIR/../conf/:$DIR/../misc/launcher/target/launcher.jar:.

pushd $DIR/..
pwd

export HOSTNAME=`hostname`
export YARN_CONF_DIR="/etc/hadoop/conf"
export DAAL_LIB_DIR="$DIR/../daal/"

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

echo java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" -Djava.library.path=$LD_LIBRARY_PATH org.trustedanalytics.atk.component.Boot rest-server
java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" -Djava.library.path=$LD_LIBRARY_PATH org.trustedanalytics.atk.component.Boot rest-server

popd
