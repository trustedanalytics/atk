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

echo "Starting ATK startup script"

set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"
export KEYTAB=$DIR/../atk.keytab
export KRB5_CONFIG=$DIR/../krb5.conf
export ATK_CONF_DIR="$DIR/../conf"
export YARN_CONF_DIR=$ATK_CONF_DIR

echo $DIR

LAUNCHER=$DIR/../launcher.jar
LAUNCHER=$DIR/../conf/logback.xml:$LAUNCHER
LAUNCHER=$DIR/../conf:$LAUNCHER
echo "Downloading jquery exectuable to parse environment variables"

jq=$DIR/../jq
echo "make jq executable"
chmod +x $jq


echo "Setting environment variables"
export APP_NAME=$(echo $VCAP_APPLICATION | $jq -r .application_name)
export APP_SPACE=$(echo $VCAP_APPLICATION | $jq -r .space_id)
export USE_HTTP=true

export FS_ROOT=$(echo $VCAP_SERVICES |  $jq -c -r '.hdfs[0].credentials.HADOOP_CONFIG_KEY["fs.defaultFS"]')
export SPARK_EVENT_LOG_DIR=$(echo $FS_ROOT | cut -d'/' -f1-3)$"/user/spark/applicationHistory"

export PRINCIPAL=$(echo $VCAP_SERVICES | $jq -c -r '.hdfs[0].credentials.HADOOP_CONFIG_KEY["dfs.datanode.kerberos.principal"]')

export ZOOKEEPER_HOST=$(echo $VCAP_SERVICES | $jq '.["zookeeper-wssb"] | .[0].credentials.uri  / "," | map(. / ":" | .[0]) | join(",")'  | tr -d '"')
export ZOOKEEPER_PORT=$(echo $VCAP_SERVICES | $jq '.["zookeeper-wssb"] | .[0].credentials.uri / "," | .[0] / ":" | .[1]' | tr -d '"')

export PG_HOST=$(echo $VCAP_SERVICES | $jq -c -r '.postgresql93 | .[0].credentials.hostname')
export PG_PORT=$(echo $VCAP_SERVICES | $jq -c -r '.postgresql93 | .[0].credentials.port')
export PG_USER=$(echo $VCAP_SERVICES | $jq -c -r '.postgresql93 | .[0].credentials.username')
export PG_PASS=$(echo $VCAP_SERVICES | $jq -c -r '.postgresql93 | .[0].credentials.password')
export PG_DB=$(echo $VCAP_SERVICES | $jq -c -r '.postgresql93 | .[0].credentials.dbname')
export PG_URL=$(echo $VCAP_SERVICES | $jq -c -r '.postgresql93 | .[0].credentials.uri')

export POSTGRES_HOST=$PG_HOST
export POSTGRES_PORT=$PG_PORT
export POSTGRES_USER=$PG_USER
export POSTGRES_PASS=$PG_PASS
export POSTGRES_DB=$PG_DB
export POSTGRES_URL=$PG_URL

export MYSQL_HOST=$(echo $VCAP_SERVICES | $jq -c -r '.mysql56 | .[0].credentials.hostname')
export MYSQL_PORT=$(echo $VCAP_SERVICES | $jq -c -r '.mysql56 | .[0].credentials.port')
export MYSQL_USER=$(echo $VCAP_SERVICES | $jq -c -r '.mysql56 | .[0].credentials.username')
export MYSQL_PASS=$(echo $VCAP_SERVICES | $jq -c -r '.mysql56 | .[0].credentials.password')
export MYSQL_DB=$(echo $VCAP_SERVICES | $jq -c -r '.mysql56 | .[0].credentials.dbname')
export MYSQL_URL=$(echo $VCAP_SERVICES | $jq -c -r '.mysql56 | .[0].credentials.uri')
env

pushd $ATK_CONF_DIR

configurationStart="<configuration>"
configurationEnd="</configuration>"
propertyStart="<property>"
propertyEnd="</property>"
nameStart="<name>"
nameEnd="</name>"
valueStart="<value>"
valueEnd="</value>"
tab="    "

hbase_file="hbase-site.xml"
hdfs_file="hdfs-site.xml"
yarn_file="yarn-site.xml"
yarn_json="yarn.json"
hdfs_json="hdfs.json"
hbase_json="hbase.json"

echo $VCAP_SERVICES |  $jq -c '.hbase[0].credentials.HADOOP_CONFIG_KEY' > $hbase_json
echo $VCAP_SERVICES |  $jq -c '.hdfs[0].credentials.HADOOP_CONFIG_KEY' > $hdfs_json
echo $VCAP_SERVICES |  $jq -c '.yarn[0].credentials.HADOOP_CONFIG_KEY' > $yarn_json

function buildSvcBrokerConfig {
(echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
 echo $configurationStart
) >> $1
key_count=$(cat $2 | $jq -c 'keys' | $jq 'length' )
keys=$(cat $2 | $jq -c 'keys')
count=0
while [ $count -lt $key_count ]
do
	key=$( echo $keys | $jq -c -r ".[$count]")
	value=$( cat $2 | $jq -c  ".[\"$key\"]" | sed -e "s|\"||g" )
	count=$((count+1))
	echo $tab$propertyStart$nameStart${key}$nameEnd$valueStart${value}$valueEnd$propertyEnd >> $1
done
echo $configurationEnd >> $1
rm $2
}

buildSvcBrokerConfig $hbase_file $hbase_json
buildSvcBrokerConfig $hdfs_file $hdfs_json
buildSvcBrokerConfig $yarn_file $yarn_json

popd

pushd $DIR/..
pwd
export PWD=`pwd`

export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

jars="engine-core.jar giraph-plugins.jar frame-plugins.jar graph-plugins.jar model-plugins.jar"

if [ -f "daal.tar.gz" ]; then
 tar -xvzf daal.tar.gz
 export DAAL_LIB_DIR="$DIR/../daal/"
 jars="$jars daal-plugins.jar"
fi

echo "Creating jar links"
for jar in $jars
do
if [ -f $DIR/../lib/$jar ]; then
   rm $DIR/../lib/$jar
 fi

 ln -s $DIR/../lib/deploy.jar $DIR/../lib/$jar
done

if [ -f ${KRB5_CONFIG} ]; then
 export JAVA_KRB_CONF="-Djava.security.krb5.conf=${KRB5_CONFIG}"
fi

echo java $@ -XX:MaxPermSize=384m $JAVA_KRB_CONF -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot rest-server
java $@ -XX:MaxPermSize=384m $JAVA_KRB_CONF -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot rest-server

popd

