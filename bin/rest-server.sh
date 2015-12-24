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

export MAVEN_REPO=~/.m2/repository
export CP=$DIR/../conf/:/etc/hadoop/conf:/etc/hbase/conf:$DIR/../module-loader/target/module-loader-master-SNAPSHOT.jar:$MAVEN_REPO/org/scala-lang/scala-library/2.10.4/scala-library-2.10.4.jar:$MAVEN_REPO/com/typesafe/config/1.2.1/config-1.2.1.jar:$MAVEN_REPO/org/scala-lang/scala-reflect/2.10.4/scala-reflect-2.10.4.jar

export BASEDIR=$DIR/..

pushd $DIR/..
pwd

export SEARCH_PATH="-Datk.module-loader.search-path=${BASEDIR}/module-loader:${BASEDIR}/rest-server:${BASEDIR}/engine:${BASEDIR}/engine-plugins:${BASEDIR}/model-publish-format:${HOME}/.m2/"
export HOSTNAME=`hostname`
export YARN_CONF_DIR="/etc/hadoop/conf"

if [ -d "${BASEDIR}/engine-plugins/daal-plugins/lib/intel64_lin" ]; then
 echo "Adding Intel DAAL libraries"
 export DAAL_LIB_DIR="${BASEDIR}/engine-plugins/daal-plugins/lib/intel64_lin"
 export DAAL_GCC_VERSION="gcc4.4"
 export LD_LIBRARY_PATH=${DAAL_LIB_DIR}:${DAAL_LIB_DIR}/${DAAL_GCC_VERSION}:${LD_LIBRARY_PATH}
fi

# needed for Python UDFs to work locally
if [ -z "$SPARK_HOME" ]
then
    export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark/
fi

echo "$NAME SPARK_HOME=$SPARK_HOME"

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

CMD=`echo java $@ -XX:MaxPermSize=384m $SEARCH_PATH -cp "$CP" -Djava.library.path=$LD_LIBRARY_PATH org.trustedanalytics.atk.moduleloader.Module rest-server org.trustedanalytics.atk.rest.RestServerApplication`
echo $CMD
$CMD

popd
