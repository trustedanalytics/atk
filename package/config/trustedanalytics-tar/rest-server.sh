#!/bin/bash
echo "Starting ATK startup script"

set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"
export KEYTAB=$DIR/../atk.keytab
export KRB5_CONFIG=$DIR/../krb5.conf
export PRINCIPAL="atk-user@US-WEST-2.COMPUTE.INTERNAL"
export ATK_CONF_DIR="$DIR/../conf"
export YARN_CONF_DIR=$ATK_CONF_DIR

echo $DIR

CP=$DIR/../lib/module-loader-master-SNAPSHOT.jar:$DIR/../lib/scala-library-2.10.4.jar:$DIR/../lib/config-1.2.1.jar:$DIR/../lib/scala-reflect-2.10.4.jar
CP=$DIR/../conf/logback.xml:$CP
CP=$DIR/../conf:$CP

export SEARCH_PATH="-Datk.module-loader.search-path=$DIR/../lib/"

echo "Downloading jquery exectuable to parse environment variables"

jq=$DIR/../jq
echo "make jq executable"
chmod +x $jq


echo "Setting environment variables"
export APP_NAME=$(echo $VCAP_APPLICATION | $jq -r .application_name)
export APP_SPACE=$(echo $VCAP_APPLICATION | $jq -r .space_id)
export USE_HTTP=true

export FS_ROOT=$(echo $VCAP_SERVICES |  $jq '.cdh | .[0].credentials.hdfs_root' | tr -d '"')
export SPARK_EVENT_LOG_DIR=$(echo $FS_ROOT | cut -d'/' -f1-3)$"/user/spark/applicationHistory"

export ZOOKEEPER_HOST=$(echo $VCAP_SERVICES | $jq '.zookeeper | .[0].credentials.uri  / "," | map(. / ":" | .[0]) | join(",")'  | tr -d '"')
export ZOOKEEPER_PORT=$(echo $VCAP_SERVICES | $jq '.zookeeper | .[0].credentials.uri / "," | .[0] / ":" | .[1]' | tr -d '"')

export PG_HOST=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.hostname' | tr -d '"')
export PG_PORT=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.port' | tr -d '"')
export PG_USER=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.username' | tr -d '"')
export PG_PASS=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.password' | tr -d '"')
export PG_DB=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.dbname' | tr -d '"')
export PG_URL=$(echo $VCAP_SERVICES | $jq '.postgresql93 | .[0].credentials.uri' | tr -d '"')

export POSTGRES_HOST=$PG_HOST
export POSTGRES_PORT=$PG_PORT
export POSTGRES_USER=$PG_USER
export POSTGRES_PASS=$PG_PASS
export POSTGRES_DB=$PG_DB
export POSTGRES_URL=$PG_URL

export MYSQL_HOST=$(echo $VCAP_SERVICES | $jq '.mysql56 | .[0].credentials.hostname' | tr -d '"')
export MYSQL_PORT=$(echo $VCAP_SERVICES | $jq '.mysql56 | .[0].credentials.port' | tr -d '"')
export MYSQL_USER=$(echo $VCAP_SERVICES | $jq '.mysql56 | .[0].credentials.username' | tr -d '"')
export MYSQL_PASS=$(echo $VCAP_SERVICES | $jq '.mysql56 | .[0].credentials.password' | tr -d '"')
export MYSQL_DB=$(echo $VCAP_SERVICES | $jq '.mysql56 | .[0].credentials.dbname' | tr -d '"')
export MYSQL_URL=$(echo $VCAP_SERVICES | $jq '.mysql56 | .[0].credentials.uri' | tr -d '"')
env

pushd $ATK_CONF_DIR
echo "Downloading yarn, hdfs and hbase configs"
for url_suffix in "yarn_config" "hdfs_config" "hbase_config"
do
  conf_url=$(echo $VCAP_SERVICES |  $jq '.cdh | .[0].credentials.'$url_suffix | tr -d '"')
  zip_file=conf_$url_suffix.zip
  curl -X GET -H "content-type:application/json" $conf_url > zip_file
  unzip -o -j zip_file
  unlink zip_file
done
popd


pushd $DIR/..
pwd
export PWD=`pwd`

export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

if [ -f ${KRB5_CONFIG} ]; then
 export JAVA_KRB_CONF="-Djava.security.krb5.conf=${KRB5_CONFIG}"
fi

echo java $@ -XX:MaxPermSize=384m $SEARCH_PATH $JAVA_KRB_CONF -cp "$CP" org.trustedanalytics.atk.moduleloader.Module rest-server org.trustedanalytics.atk.rest.RestServerApplication
java $@ -XX:MaxPermSize=384m $SEARCH_PATH $JAVA_KRB_CONF -cp "$CP" org.trustedanalytics.atk.moduleloader.Module rest-server org.trustedanalytics.atk.rest.RestServerApplication

popd

