#!/bin/bash
#
# This is a special version of the REST Server start-up script for integration testing on a build machine
#

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "$NAME DIR=$DIR"

CONFDIR=$DIR/conf

if [[ -f $DIR/../misc/launcher/target/launcher.jar ]]; then
	LAUNCHER=$DIR/../misc/launcher/target/launcher.jar
fi


# EXTRA_CLASSPATH is not used in this script
CONF="$CONFDIR"

pushd $DIR/..
pwd

export HOSTNAME=`hostname`

PORT=19099
echo "$NAME making sure port $PORT isn't already in use"
server=$(netstat -tuln | grep  :$PORT)
if [ "$server" != "" ]
then
    echo "$NAME ERROR: Port $PORT is already in use!!! (it can take a little while for it to be released, we should switch to random port)"
    exit 2
else
    echo "$NAME Port $PORT is free"
fi

export TARGET_DIR=$DIR/target
export FS_ROOT=$TARGET_DIR/fs-root
LOG=$TARGET_DIR/rest-server.log

mkdir -p $FS_ROOT

export SPARK_EVENTS_DIR=$TARGET_DIR/spark-events
mkdir -p $SPARK_EVENTS_DIR

echo "$NAME remove old rest-server.log and datasets from target dir"
rm -f $LOG $TARGET_DIR/datasets

echo "$NAME copying datasets to target"
cp -rp $DIR/datasets $FS_ROOT

echo "$NAME fs.root is $FS_ROOT"
echo "$NAME Api Server logging going to $LOG"

echo "starting"
java $@ -XX:MaxPermSize=256m -Xss10m -cp "$CONF:$LAUNCHER" \
    -javaagent:$DIR/../lib/aspectjweaver-1.8.6.jar \
    -Dconfig.resource=integration-test.conf \
    -Dtrustedanalytics.atk.engine.fs.root=file:$FS_ROOT \
    -Dtrustedanalytics.atk.engine.spark.conf.properties.spark.sql.parquet.useDataSourceApi=false \
    -Dtrustedanalytics.atk.engine.spark.conf.properties.spark.eventLog.dir=file:$SPARK_EVENTS_DIR\
    org.trustedanalytics.atk.component.Boot rest-server org.trustedanalytics.atk.rest.RestServerApplication > $LOG 2>&1 &

API_SERVER_PID=$!

echo $API_SERVER_PID > $TARGET_DIR/rest-server.pid
popd
