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
