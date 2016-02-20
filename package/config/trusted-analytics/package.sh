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

workDir=$(pwd)
baseDir=${workDir##*/}
gitRoot="."
pwd
$baseDir

if [ "$baseDir" == "package" ]; then
	source common.sh
	gitRoot=".."
	else
	source package/common.sh
	gitRoot="."
fi
echo $gitRoot

packageName=$1
VERSION=$VERSION
BUILD_DIR=$BUILD_DIR

echo $packageName
echo $VERSION
echo $BUILD_DIR

pwd
echo $SCRIPTPATH

log "packageName: $packageName"
#call package.sh for rest-server
package trustedanalytics-rest-server
packageName=$1
cp ${BUILD_DIR}/etc/trustedanalytics/rest-server/parcel.conf.tpl  ${BUILD_DIR}/etc/trustedanalytics/rest-server/application.conf

log "packageName: $packageName"
#call package.sh for rest server
package trustedanalytics-python-rest-client
packageName=$1

log "packageName: $packageName"
#call package.sh for spark-deps
package trustedanalytics-spark-deps
packageName=$1


#call package.sh for client
log "packageName: $packageName"
createArchive $packageName


