#!/bin/bash
#set -o errexit
DIR="$( cd "$( dirname "$0" )" && pwd )"

for jar in `find $DIR/../package/config/trustedanalytics/assets/etc/trustedanalytics/rest-server -name "*.jar"`
do
    cp $jar $DIR/../conf
done

LAUNCHER=$DIR/../conf/:$DIR/../misc/launcher/target/launcher.jar:.

pushd $DIR/..
pwd

export HOSTNAME=`hostname`
export YARN_CONF_DIR="/etc/hadoop/conf"

# NOTE: Add this parameter to Java for connecting to a debugger
# -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005

echo java $@ -XX:MaxPermSize=384m -javaagent:$DIR/../conf/aspectjweaver-1.8.6.jar -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot rest-server
java $@ -XX:MaxPermSize=384m -javaagent:$DIR/../conf/aspectjweaver-1.8.6.jar -cp "$LAUNCHER" org.trustedanalytics.atk.component.Boot rest-server

popd
