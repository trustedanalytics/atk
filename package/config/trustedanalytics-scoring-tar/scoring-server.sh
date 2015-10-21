#!/bin/bash
echo "Starting Simple Scoring Engine startup script"

set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

#export ATK_CONF_DIR="$DIR/../conf"
echo $DIR

LAUNCHER=$DIR/../launcher.jar
LAUNCHER=$DIR/../conf:$LAUNCHER

env

pushd $DIR/..
pwd
export PWD=`pwd`

export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

echo java $@ -Xmx2G -XX:+HeapDumpOnOutOfMemoryError -XX:MaxMetaspaceSize=500m -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot scoring-engine
java $@ -Xmx4G -XX:MaxMetaspaceSize=2G -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot scoring-engine

popd
