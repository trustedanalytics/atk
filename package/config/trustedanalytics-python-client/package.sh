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

mkdir -p  ${BUILD_DIR}/usr/lib/trustedanalytics/python-client

pushd $SCRIPTPATH
    cp -v requirements-linux.txt ${BUILD_DIR}/usr/lib/trustedanalytics/python-client/
popd


log "delete pyc files"
find ../python-client/trustedanalytics -name *.pyc -type f -delete
log "regular package"
cp -Rv  ../python-client/trustedanalytics/* ${BUILD_DIR}/usr/lib/trustedanalytics/python-client/


mkdir -p ${BUILD_DIR}/usr/lib/trustedanalytics/python-client/examples/

cp -Rv ../python-client/examples/end-user/* ${BUILD_DIR}/usr/lib/trustedanalytics/python-client/examples/

cp -Rv  ../python-client/cmdgen.py ${BUILD_DIR}/usr/lib/trustedanalytics/python-client/


createArchive $packageName
