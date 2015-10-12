#!/bin/bash
workDir=$(pwd)
baseDir=${workDir##*/}
gitRoot="."
if [ "$baseDir" == "package" ]; then
	source common.sh
	gitRoot=".."
	else
	source package/common.sh
	gitRoot="."
fi

packageName=$1
VERSION=$VERSION
BUILD_DIR=$BUILD_DIR

echo $packageName
echo $VERSION
echo $BUILD_DIR

echo "$SCRIPTPATH"

pwd

mkdir -p  ${BUILD_DIR}/etc/trustedanalytics/rest-server
mkdir -p  ${BUILD_DIR}/usr/lib/trustedanalytics/rest-server/lib

#copy example scripts
mkdir -p ${BUILD_DIR}/usr/lib/trustedanalytics/rest-server/examples/
cp -Rv ../python-client/examples/end-user/* ${BUILD_DIR}/usr/lib/trustedanalytics/rest-server/examples/

if [ -d /home/agent/datasets ]; then
    #copy datasets from agent home if it exists into the rpm tar.gz source
    cp -Rv /home/agent/datasets ${BUILD_DIR}/usr/lib/trustedanalytics/rest-server/examples
fi


cp -v ../conf/examples/application.conf.tpl ${BUILD_DIR}/etc/trustedanalytics/rest-server
cp -v ../conf/examples/parcel.conf.tpl      ${BUILD_DIR}/etc/trustedanalytics/rest-server
cp -v ../conf/examples/application.conf.single-system.tpl ${BUILD_DIR}/etc/trustedanalytics/rest-server

pushd $SCRIPTPATH
    cp -Rv assets/* ${BUILD_DIR}
popd

echo "copy jar dependencies"
cp -v rest-server-lib/target/dependency/*.jar ${BUILD_DIR}/usr/lib/trustedanalytics/rest-server/lib/

mkdir -p ${BUILD_DIR}/usr/lib/trustedanalytics/rest-server/client
ls -l config/trustedanalytics-python-client/trustedanalytics/dist
cp -v config/trustedanalytics-python-client/trustedanalytics/dist/trustedanalytics*.tar.gz ${BUILD_DIR}/usr/lib/trustedanalytics/rest-server/client
ls -l ../python-client/target
cp -v ../python-client/target/trustedanalytics.zip ${BUILD_DIR}/usr/lib/trustedanalytics/rest-server/lib

popd

log "createArchive $packageName"
createArchive $packageName
