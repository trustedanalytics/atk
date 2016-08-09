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
echo "Current Execution Directory:"$DIR

jq=$DIR/../jq
echo "make jq executable"
chmod +x $jq

export ATK_CONF_DIR="$DIR/../conf"
export YARN_CONF_DIR=$ATK_CONF_DIR
export HADOOP_CONF_DIR=$YARN_CONF_DIR


CP=$DIR/../lib/module-loader-master-SNAPSHOT.jar:$DIR/../lib/scala-library-2.10.5.jar:$DIR/../lib/config-1.2.1.jar:$DIR/../lib/scala-reflect-2.10.5.jar
CP=$DIR/../conf/logback.xml:$CP
CP=$DIR/../conf:$CP

export SEARCH_PATH="-Datk.module-loader.search-path=$DIR/../lib/"

echo "Setting environment variables"
export APP_NAME=$(echo $VCAP_APPLICATION | $jq -r .application_name)
export APP_SPACE=$(echo $VCAP_APPLICATION | $jq -r .space_id)
export USE_HTTP=true

export FS_ROOT=$(echo $VCAP_SERVICES |  $jq -c -r '.hdfs[0].credentials.uri')
export FS_TECHNICAL_USER_NAME=$(echo $VCAP_SERVICES |  $jq -c -r '.hdfs[0].credentials.user')
export FS_TECHNICAL_USER_PASSWORD=$(echo $VCAP_SERVICES |  $jq -c -r '.hdfs[0].credentials.password')
export SPARK_EVENT_LOG_DIR=$(echo $FS_ROOT | cut -d'/' -f1-3)$"/user/spark/applicationHistory"

export KRB5_CONFIG=$DIR/../krb5jwt/etc/krb5.conf
export USE_SUBJECT_CREDS="-Djavax.security.auth.useSubjectCredsOnly=false"
export JAVA_KRB_CONF="-Djava.security.krb5.conf=${KRB5_CONFIG}"

# Make a curl request to UAA and get the PRINCIPAL name for the cloud foundry technical user
get_technical_user_id() {

    access_token=`curl -X POST  http://${UAA_URI}/oauth/token -H "Accept:application/json" -H \
    "Content-Type:application/x-www-form-urlencoded" -d \
    "grant_type=password&password=${FS_TECHNICAL_USER_PASSWORD}&scope=&username=${FS_TECHNICAL_USER_NAME}" -u \
    ${UAA_CLIENT_NAME}:${UAA_CLIENT_PASSWORD} | $jq -c -r '.access_token' | cut -d'.' -f2`

    technical_user_id=`base64 -d <<< $access_token | $jq -c -r '.user_id'`
    echo $technical_user_id
}

kerberos_realm=$(echo $VCAP_SERVICES |  $jq -c -r '.hdfs[0].credentials.kerberos.krealm')
export KRB5CCNAME="/tmp/"$( get_technical_user_id )"@${kerberos_realm}"
export HADOOP_USER_NAME=$( get_technical_user_id )
export HBASE_NAMESPACE=$(echo $VCAP_SERVICES |  $jq -c -r '.hbase[0].credentials."hbase.namespace"')
export HIVE_CONNECTION_URL=$(echo $VCAP_SERVICES |  $jq -c -r '.hive[0].credentials.connectionUrl')

# uncomment the following lines if a binding to the zookeeper is needed
#export ZOOKEEPER_HOST=$(echo $VCAP_SERVICES | $jq '.["zookeeper-wssb"] | .[0].credentials.uri  / "," | map(. / ":" | .[0]) | join(",")'  | tr -d '"')
#export ZOOKEEPER_PORT=$(echo $VCAP_SERVICES | $jq '.["zookeeper-wssb"] | .[0].credentials.uri / "," | .[0] / ":" | .[1]' | tr -d '"')

export PG_CREDENTIALS=$(echo $VCAP_SERVICES | $jq -c -r '. | to_entries[] | select(.key | startswith("postgresql")) | .value[0].credentials')
export PG_HOST=$(echo $PG_CREDENTIALS | $jq -c -r '.hostname')
export PG_PORT=$(echo $PG_CREDENTIALS | $jq -c -r '.port')
export PG_USER=$(echo $PG_CREDENTIALS | $jq -c -r '.username')
export PG_PASS=$(echo $PG_CREDENTIALS | $jq -c -r '.password')
export PG_DB=$(echo $PG_CREDENTIALS | $jq -c -r '.dbname')
export PG_URL=$(echo $PG_CREDENTIALS | $jq -c -r '.uri')

export DATA_CATALOG_PREFIX="data-catalog."
export TAP_DOMAIN=$(echo $VCAP_APPLICATION | $jq --raw-output -r '.uris[0][.application_name|length + 1:]')
export DATA_CATALOG_URI=$DATA_CATALOG_PREFIX$TAP_DOMAIN


export POSTGRES_HOST=$PG_HOST
export POSTGRES_PORT=$PG_PORT
export POSTGRES_USER=$PG_USER
export POSTGRES_PASS=$PG_PASS
export POSTGRES_DB=$PG_DB
export POSTGRES_URL=$PG_URL

#OrientDB
export ORIENTDB_HOST=$(echo $VCAP_SERVICES | $jq -c -r '.orientdb | .[0].credentials.hostname')
export ORIENTDB_PORT=$(echo $VCAP_SERVICES | $jq -c -r '.orientdb | .[0].credentials.ports["2424/tcp"]')
export ORIENTDB_ROOTPASS=$(echo $VCAP_SERVICES | $jq -c -r '.orientdb | .[0].credentials.password')



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
hive_file="hive-site.xml"
yarn_json="yarn.json"
hdfs_json="hdfs.json"
hbase_json="hbase.json"
hive_json="hive.json"

echo $VCAP_SERVICES |  $jq -c '.hbase[0].credentials.HADOOP_CONFIG_KEY' > $hbase_json
echo $VCAP_SERVICES |  $jq -c '.hdfs[0].credentials.HADOOP_CONFIG_KEY' > $hdfs_json
echo $VCAP_SERVICES |  $jq -c '.yarn[0].credentials.HADOOP_CONFIG_KEY' > $yarn_json
echo $VCAP_SERVICES |  $jq -c '.hive[0].credentials.HADOOP_CONFIG_KEY' > $hive_json

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
buildSvcBrokerConfig $hive_file $hive_json

## This next 2 blocks are temporary fixes for incorrect VCAP parameters passed through brokers and ublock new module-loader
yarn_application_classpath="<property><name>yarn.application.classpath</name><value>,,/*,/lib/*,/*,/lib/*,/*,/lib/*</value></property>"
safe_yarn_application_classpath=$(printf '%s\n' "$yarn_application_classpath" | sed 's/[[\.*^$(){}?+|/]/\\&/g')
correct_yarn_application_classpath="<property><name>yarn.application.classpath</name><value>\$HADOOP_CONF_DIR,\$HADOOP_COMMON_HOME/*,\$HADOOP_COMMON_HOME/lib/*,\$HADOOP_HDFS_HOME/*,\$HADOOP_HDFS_HOME/lib/*,\$HADOOP_YARN_HOME/*,\$HADOOP_YARN_HOME/lib/*</value></property>"
safe_correct_yarn_application_classpath=$(printf '%s\n' "$correct_yarn_application_classpath" | sed 's/[[\.*^$(){}?+|/]/\\&/g')
sed -i s/$safe_yarn_application_classpath/$safe_correct_yarn_application_classpath/g yarn-site.xml

mapreduce_application_classpath="<property><name>mapreduce.application.classpath</name><value>/*,/lib/*,</value></property>"
safe_mapreduce_application_classpath=$(printf '%s\n' "$mapreduce_application_classpath" | sed 's/[[\.*^$(){}?+|/]/\\&/g')
new_mapreduce_application_classpath="<property><name>mapreduce.application.classpath</name><value>\$HADOOP_MAPRED_HOME/*,\$HADOOP_MAPRED_HOME/lib/*,\$MR2_CLASSPATH</value></property>"
safe_correct_mapreduce_application_classpath=$(printf '%s\n' "$new_mapreduce_application_classpath" | sed 's/[[\.*^$(){}?+|/]/\\&/g')
sed -i s/$safe_mapreduce_application_classpath/$safe_correct_mapreduce_application_classpath/g yarn-site.xml

popd

pushd $DIR/..
pwd
export PWD=`pwd`

export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

if [ -d "$DIR/../lib/daal/intel64_lin" ]; then
 export DAAL_LIB_DIR="$DIR/../lib/daal/intel64_lin"
 export DAAL_GCC_VERSION="gcc4.4"
 export LD_LIBRARY_PATH=${DAAL_LIB_DIR}:${DAAL_LIB_DIR}/${DAAL_GCC_VERSION}:${LD_LIBRARY_PATH}
fi

echo java $@ $JAVA_OPTS -XX:MaxPermSize=384m $SEARCH_PATH $USE_SUBJECT_CREDS -cp "$CP" -Djava.library.path=$LD_LIBRARY_PATH org.trustedanalytics.atk.moduleloader.Module rest-server org.trustedanalytics.atk.rest.RestServerApplication
java $@ $JAVA_OPTS -XX:MaxPermSize=384m $SEARCH_PATH $USE_SUBJECT_CREDS -cp "$CP" -Djava.library.path=$LD_LIBRARY_PATH org.trustedanalytics.atk.moduleloader.Module rest-server org.trustedanalytics.atk.rest.RestServerApplication

popd

