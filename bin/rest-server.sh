#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

export MAVEN_REPO=~/.m2/repository
export CP=$DIR/../conf/:/etc/hadoop/conf:/etc/hbase/conf:$DIR/../module-loader/target/module-loader-master-SNAPSHOT.jar:$MAVEN_REPO/org/scala-lang/scala-library/2.10.4/scala-library-2.10.4.jar:$MAVEN_REPO/com/typesafe/config/1.2.1/config-1.2.1.jar:$MAVEN_REPO/org/scala-lang/scala-reflect/2.10.4/scala-reflect-2.10.4.jar

pushd $DIR/..
pwd

export SEARCH_PATH="-Datk.module-loader.search-path=module-loader/target:rest-server/target:engine/engine-core/target:engine/interfaces/target:engine-plugins/frame-plugins/target:engine-plugins/graph-plugins/target:engine-plugins/model-plugins/target:engine-plugins/giraph-plugins/target"
export HOSTNAME=`hostname`
export YARN_CONF_DIR="/etc/hadoop/conf"

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

CMD=`echo java $@ -XX:MaxPermSize=384m $SEARCH_PATH -cp "$CP" org.trustedanalytics.atk.moduleloader.Module rest-server org.trustedanalytics.atk.rest.RestServerApplication`
echo $CMD
$CMD

popd
