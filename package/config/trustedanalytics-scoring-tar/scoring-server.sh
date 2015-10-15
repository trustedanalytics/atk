#!/bin/bash
echo "Starting Simple Scoring Engine startup script"

set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

#export ATK_CONF_DIR="$DIR/../conf"
echo $DIR

CP=$DIR/../lib/module-loader-master-SNAPSHOT.jar:$DIR/../lib/scala-library-2.10.4.jar:$DIR/../lib/config-1.2.1.jar:$DIR/../lib/scala-reflect-2.10.4.jar
CP=$DIR/../conf/logback.xml:$CP

export SEARCH_PATH="-Datk.module-loader.search-path=$DIR/../lib/"

echo "Downloading jquery exectuable to parse environment variables"
jq=$DIR/../jq
wget http://stedolan.github.io/jq/download/linux64/jq -O $jq
chmod +x $jq

echo "Setting environment variables"
export APP_NAME=$(echo $VCAP_APPLICATION | $jq -r .application_name)
export APP_SPACE=$(echo $VCAP_APPLICATION | $jq -r .space_id)
export CC_URI=$(echo $VCAP_APPLICATION | $jq  '.application_uris[0]' | sed -e "s/$APP_NAME\.apps/api.run/g" | tr -d '"')
export UAA_URI=$(echo $VCAP_APPLICATION | $jq  '.application_uris[0]' | sed -e "s/$APP_NAME\.apps/uaa.run/g" | tr -d '"')

export FS_ROOT=$(echo $VCAP_SERVICES |  $jq '.cdh | .[0].credentials.hdfs_root' | tr -d '"')

env

pushd $DIR/..
pwd
export PWD=`pwd`

export PATH=$PWD/.java-buildpack/open_jdk_jre/bin:$PATH
export JAVA_HOME=$PWD/.java-buildpack/open_jdk_jre

echo java $@ -XX:MaxPermSize=256m $SEARCH_PATH -cp "$CP" org.trustedanalytics.atk.moduleloader.Module scoring-engine org.trustedanalytics.atk.scoring.ScoringServiceApplication
java $@ -XX:MaxPermSize=256m $SEARCH_PATH -cp "$CP" org.trustedanalytics.atk.moduleloader.Module scoring-engine org.trustedanalytics.atk.scoring.ScoringServiceApplication

popd
