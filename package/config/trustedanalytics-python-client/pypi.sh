#!/bin/bash
#
#  Copyright (c) 2016 Intel Corporation 
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

cp -Rv ${BUILD_DIR}/usr/lib/trustedanalytics/python-client/

cp -Rv ${BUILD_DIR}/usr/lib/trustedanalytics/python-client/* trustedanalytics/trustedanalytics

rm -rf usr

#copy assest files
cp -Rv assets/* trustedanalytics/
cp -v  requirements-windows.txt trustedanalytics/
cp -v  requirements-linux.txt trustedanalytics/

pushd trustedanalytics


sed -i "s|VERSION|$version|g" setup.py

weekly=$(echo $BRANCH | grep "w[0-9]*$")
if [ "$weekly" == "" ]; then
    sed -i "s|POSTTAG|${DAY}${BUILD_NUMBER}|g" setup.py
else
    sed -i "s|POSTTAG|dev${DAY}${BUILD_NUMBER}|g" setup.py
fi

cat setup.py

python setup.py sdist

popd

popd
