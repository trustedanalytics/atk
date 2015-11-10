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
export PRINCIPAL=$(echo $VCAP_SERVICES | $jq -c -r '.hdfs[0].credentials.HADOOP_CONFIG_KEY["dfs.datanode.kerberos.principal"]')

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

hdfs_file="hdfs-site.xml"
hdfs_json="hdfs.json"

echo $VCAP_SERVICES |  $jq -c '.hdfs[0].credentials.HADOOP_CONFIG_KEY' > $hdfs_json

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

buildSvcBrokerConfig $hdfs_file $hdfs_json

popd

pushd $DIR/..
pwd
export PWD=`pwd`

export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

if [ -f ${KRB5_CONFIG} ]; then
 export JAVA_KRB_CONF="-Djava.security.krb5.conf=${KRB5_CONFIG}"
fi

echo java $@ -XX:MaxPermSize=384m $JAVA_KRB_CONF -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot scoring-engine
java $@ -XX:MaxPermSize=384m $JAVA_KRB_CONF -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot scoring-engine

popd

