#!/bin/bash
echo "Starting ATK startup script"

set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"
export KEYTAB=$DIR/../atk.keytab
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

export FS_ROOT=$(echo $VCAP_SERVICES |  $jq -c -r '.cdh | .[0].credentials.hdfs_root')
export SPARK_EVENT_LOG_DIR=$(echo $FS_ROOT | cut -d'/' -f1-3)$"/user/spark/applicationHistory"

export PRINCIPAL=$(echo $VCAP_SERVICES | $jq -c -r '.hdfs[0].credentials.HADOOP_CONFIG_KEY["dfs.datanode.kerberos.principal"]')

export ZOOKEEPER_HOST=$(echo $VCAP_SERVICES | $jq -c -r '.zookeeper[0].credentials["zk.cluster"] / "," | map(. / ":" | .[0]) | join(",")')
export ZOOKEEPER_PORT=$(echo $VCAP_SERVICES | $jq -c -r '.zookeeper[0].credentials["zk.cluster"] / "," | .[0] / ":" | .[1]')

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
echo "Downloading yarn, hdfs and hbase configs"
for url_suffix in "yarn_config" "hdfs_config" "hbase_config"
do
  conf_url=$(echo $VCAP_SERVICES |  $jq -c -r '.cdh | .[0].credentials.'$url_suffix)
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

jars="engine-core.jar giraph-plugins.jar frame-plugins.jar graph-plugins.jar model-plugins.jar"
echo "Creating jar links"
for jar in $jars
do
if [ -f $DIR/../lib/$jar ]; then
   rm $DIR/../lib/$jar
 fi

 ln -s $DIR/../lib/deploy.jar $DIR/../lib/$jar
done

echo java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot rest-server
java $@ -XX:MaxPermSize=384m -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot rest-server

popd