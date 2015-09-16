#!/bin/bash
source common.sh

log "Build pypi package"

packageName=$1
tarFile=$2
TAR_FILE=$tarFile
version=$3

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

pushd $SCRIPTPATH

#create directory
mkdir -p trustedanalytics/trustedanalytics


tar -xvf $tarFile -C trustedanalytics/

cp -Rv ${BUILD_DIR}/usr/lib/trustedanalytics/python-client/* trustedanalytics/trustedanalytics

rm -rf usr

#copy assest files
cp -Rv assets/* trustedanalytics/
cp -v  requirements-windows.txt trustedanalytics/
cp -v  requirements-linux.txt trustedanalytics/

pushd trustedanalytics


sed -i "s|VERSION|$version|g" setup.py
sed -i "s|POSTTAG|${DAY}${BUILD_NUMBER}|g" setup.py

cat setup.py

python setup.py sdist

popd

popd
