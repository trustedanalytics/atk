#!/bin/bash
echo "Starting Simple Scoring Engine startup script"

set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

#export ATK_CONF_DIR="$DIR/../conf"
echo $DIR

LAUNCHER=$DIR/../launcher.jar
LAUNCHER=$DIR/../conf/logback.xml:$LAUNCHER

env

pushd $DIR/..
pwd
export PWD=`pwd`

export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

echo java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot scoring-engine
java $@ -XX:MaxPermSize=256m -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot scoring-engine

popd
