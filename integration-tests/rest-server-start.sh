#!/bin/bash
#
# This is a special version of the REST Server start-up script for integration testing on a build machine
#

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
export TARGET_DIR=$DIR/target

echo "$NAME DIR=$DIR"

CONFDIR=$DIR/conf

# was needed for Python UDF tests to pass locally
if [ -d "/usr/lib/spark/spark-cdh5.4.2-release/" ]
then
    export SPARK_HOME=/usr/lib/spark/spark-cdh5.4.2-release/
else
    export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark/
fi

export MAVEN_REPO=~/.m2/repository
export CP=$DIR/../conf/:/etc/hadoop/conf:/etc/hbase/conf:$DIR/../module-loader/target/module-loader-master-SNAPSHOT.jar:$MAVEN_REPO/org/scala-lang/scala-library/2.10.4/scala-library-2.10.4.jar:$MAVEN_REPO/com/typesafe/config/1.2.1/config-1.2.1.jar:$MAVEN_REPO/org/scala-lang/scala-reflect/2.10.4/scala-reflect-2.10.4.jar:`ls $TARGET_DIR/dependencies/*.jar | tr '\n' ':' `

export SEARCH_PATH="-Datk.module-loader.search-path=module-loader/target:rest-server/target:engine/engine-core/target:engine/interfaces/target:engine-plugins/frame-plugins/target:engine-plugins/graph-plugins/target:engine-plugins/model-plugins/target:engine-plugins/giraph-plugins/target"

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

echo "starting, CP=$CP"
java $@ -XX:MaxPermSize=256m -Xss10m $SEARCH_PATH -cp "$CONF:$CP" \
    -Dconfig.resource=integration-test.conf \
    -Dtrustedanalytics.atk.engine.fs.root=file:$FS_ROOT \
    -Dtrustedanalytics.atk.engine.spark.conf.properties.spark.sql.parquet.useDataSourceApi=false \
    -Dtrustedanalytics.atk.engine.spark.conf.properties.spark.eventLog.dir=file:$SPARK_EVENTS_DIR\
     org.trustedanalytics.atk.moduleloader.Module rest-server org.trustedanalytics.atk.rest.RestServerApplication > $LOG 2>&1 &

API_SERVER_PID=$!

echo $API_SERVER_PID > $TARGET_DIR/rest-server.pid
popd
