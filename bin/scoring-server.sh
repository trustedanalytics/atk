#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"


LAUNCHER=$DIR/../conf/:$DIR/../misc/launcher/target/launcher.jar:.

pushd $DIR/..
pwd

export HOSTNAME=`hostname`


# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

echo java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot scoring-engine
java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot scoring-engine

popd
