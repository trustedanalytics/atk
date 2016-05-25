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
pwd
package=$1
BUILD_NUMBER=$2
VERSION=$3
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

echo "$SCRIPTPATH"

rm -rf ../bin/stage
rm -rf tarballs/$package
rm $package-source.tar.gz


echo "create package directories"
mkdir -pv  tarballs/$package/bin
mkdir -pv  tarballs/$package/conf
mkdir -pv  tarballs/$package/lib
mkdir -pv  tarballs/$package/lib/daal

pwd
ls -la
if [ -f licenses-rest-server.xml ]; then
        mv licenses-rest-server.xml tarballs/$package/licenses.xml
else
        echo no license file
fi


echo "copy config into package"
cp -v  config/$package/application.conf tarballs/$package/conf
cp -v  config/$package/logback.xml tarballs/$package/conf
cp -Rv config/trustedanalytics/assets/etc/trustedanalytics/rest-server/* tarballs/$package/conf
cp -v  config/$package/rest-server.sh tarballs/$package/bin/
cp -v  config/$package/jq tarballs/$package/
cp -v  config/$package/manifest.yml.tpl tarballs/$package/

echo "copy Intel DAAL dynamic libraries into package"
tar -jxvf ../daal-utils/lib/daal-libs.tar.bz2 -C tarballs/$package/lib/daal
cp -v ../daal-utils/lib/daal-core-${DAAL_JAR_VERSION}.jar tarballs/$package/lib

echo "copy jar dependencies"
cp -v rest-server-lib/target/dependency/*.jar tarballs/$package/lib/

echo "create client"
mkdir -pv tarballs/$package/client
ls -l config/trustedanalytics-python-client/trustedanalytics/dist
cp -v config/trustedanalytics-python-client/trustedanalytics/dist/trustedanalytics*.tar.gz tarballs/$package/client
ls -l ../python-client/target
cp -v ../python-client/target/trustedanalytics.zip tarballs/$package/lib/

echo "create trustedanalytics.tar.gz"
pushd tarballs/$package
    tar -pczf ../../trustedanalytics.tar.gz .
popd


echo "remove tarballs"
rm -rf tarballs
